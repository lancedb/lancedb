# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

import lancedb
import numpy as np
import pyarrow as pa
import pytest
import pytest_asyncio

from lancedb.query import (
    AsyncFTSQuery,
    AsyncHybridQuery,
    FullTextSearchQuery,
    LanceFtsQueryBuilder,
    LanceHybridQueryBuilder,
    Query,
)
from lancedb.table import _requires_local_namespace_execution


@pytest.fixture
def db(tmp_path):
    return lancedb.connect(str(tmp_path))


@pytest.fixture
def fts_table(db):
    """Small table with an FTS index."""
    data = pa.table(
        {
            "id": [1, 2, 3],
            "text": [
                "machine learning is great",
                "deep learning rocks",
                "python rocks",
            ],
            "vector": [
                [1.0, 0.0, 0.0, 0.0],
                [0.0, 1.0, 0.0, 0.0],
                [0.0, 0.0, 1.0, 0.0],
            ],
        }
    )
    tbl = db.create_table("fts_table", data=data)
    tbl.create_fts_index("text", replace=True)
    return tbl


@pytest_asyncio.fixture
async def async_db(tmp_path):
    return await lancedb.connect_async(str(tmp_path))


@pytest_asyncio.fixture
async def async_fts_table(async_db):
    """Small table with an FTS index (async connection)."""
    data = pa.table(
        {
            "id": [1, 2, 3],
            "text": [
                "machine learning is great",
                "deep learning rocks",
                "python rocks",
            ],
            "vector": [
                [1.0, 0.0, 0.0, 0.0],
                [0.0, 1.0, 0.0, 0.0],
                [0.0, 0.0, 1.0, 0.0],
            ],
        }
    )
    tbl = await async_db.create_table("async_fts_table", data=data)
    await tbl.create_index("text", config=lancedb.index.FTS(with_position=False))
    return tbl


def test_default_wand_factor_is_none():
    """wand_factor must default to None in FullTextSearchQuery."""
    q = FullTextSearchQuery(query="hello")
    assert q.wand_factor is None


def test_fts_builder_default_wand_factor(fts_table):
    """LanceFtsQueryBuilder must not set wand_factor by default."""
    builder = LanceFtsQueryBuilder(fts_table, "machine learning")
    query_obj = builder.to_query_object()
    assert query_obj.full_text_query is not None
    assert query_obj.full_text_query.wand_factor is None


def test_fts_builder_set_wand_factor(fts_table):
    """wand_factor set on LanceFtsQueryBuilder is stored in the Query object."""
    builder = LanceFtsQueryBuilder(fts_table, "machine learning").wand_factor(1.5)
    query_obj = builder.to_query_object()
    assert query_obj.full_text_query is not None
    assert query_obj.full_text_query.wand_factor == pytest.approx(1.5)


def test_fts_builder_wand_factor_zero(fts_table):
    """wand_factor=0 should be accepted (non-negative)."""
    builder = LanceFtsQueryBuilder(fts_table, "machine learning").wand_factor(0.0)
    query_obj = builder.to_query_object()
    assert query_obj.full_text_query.wand_factor == pytest.approx(0.0)


def test_fts_builder_wand_factor_negative_raises(fts_table):
    """Negative wand_factor must raise ValueError."""
    builder = LanceFtsQueryBuilder(fts_table, "machine learning")
    with pytest.raises(ValueError, match="non-negative"):
        builder.wand_factor(-1.0)


@pytest.mark.parametrize("value", [float("nan"), float("inf"), float("-inf")])
def test_fts_builder_wand_factor_non_finite_raises(fts_table, value):
    builder = LanceFtsQueryBuilder(fts_table, "machine learning")
    with pytest.raises(ValueError, match="finite"):
        builder.wand_factor(value)


def test_fts_builder_wand_factor_returns_self(fts_table):
    """wand_factor() must be fluent (returns the builder)."""
    builder = LanceFtsQueryBuilder(fts_table, "machine learning")
    result = builder.wand_factor(1.0)
    assert result is builder


def test_fts_builder_wand_factor_none_clears(fts_table):
    """Calling wand_factor(None) should clear the value."""
    builder = (
        LanceFtsQueryBuilder(fts_table, "machine learning")
        .wand_factor(1.5)
        .wand_factor(None)
    )
    query_obj = builder.to_query_object()
    assert query_obj.full_text_query.wand_factor is None


def test_pyo3_fts_query_wand_factor(fts_table):
    """FTSQuery (PyO3) getter/setter round-trip works correctly."""
    sync_q = (
        LanceFtsQueryBuilder(fts_table, "machine learning")
        .wand_factor(2.0)
        .to_query_object()
    )
    async_tbl = fts_table._table
    async_q = async_tbl._sync_query_to_async(sync_q)
    assert isinstance(async_q, AsyncFTSQuery)
    assert async_q.get_wand_factor() == pytest.approx(2.0)


def test_pyo3_fts_query_wand_factor_roundtrip(fts_table):
    """wand_factor survives PyO3 FTSQuery getter/setter round-trip."""
    sync_q = (
        LanceFtsQueryBuilder(fts_table, "machine learning")
        .wand_factor(1.7)
        .to_query_object()
    )
    async_tbl = fts_table._table
    async_q = async_tbl._sync_query_to_async(sync_q)
    assert isinstance(async_q, AsyncFTSQuery)

    req = async_q._inner.to_query_request()
    assert hasattr(req, "wand_factor"), "PyQueryRequest is missing wand_factor field"
    assert req.wand_factor == pytest.approx(1.7)


def test_pyo3_hybrid_query_wand_factor(fts_table):
    """HybridQuery (PyO3) delegates wand_factor to its inner FTSQuery."""
    builder = (
        fts_table.search(query_type="hybrid")
        .vector([0.1, 0.2, 0.3, 0.4])
        .text("machine learning")
    )
    assert isinstance(builder, LanceHybridQueryBuilder)
    builder.wand_factor(3.0)
    assert builder._wand_factor == pytest.approx(3.0)
    builder._create_query_builders()
    fts_child = builder._fts_query
    assert isinstance(fts_child, LanceFtsQueryBuilder)
    assert fts_child._wand_factor == pytest.approx(3.0)


def test_wand_factor_roundtrip_via_query_request(fts_table):
    """wand_factor must survive a PyQueryRequest → Query.from_inner() roundtrip."""
    sync_q = (
        LanceFtsQueryBuilder(fts_table, "machine learning")
        .wand_factor(1.7)
        .to_query_object()
    )
    async_tbl = fts_table._table
    async_q = async_tbl._sync_query_to_async(sync_q)

    req = async_q._inner.to_query_request()
    assert hasattr(req, "wand_factor"), "PyQueryRequest is missing wand_factor field"
    assert req.wand_factor == pytest.approx(1.7)

    reconstructed = Query.from_inner(req)
    assert reconstructed.full_text_query is not None
    assert reconstructed.full_text_query.wand_factor == pytest.approx(1.7)


def test_wand_factor_absent_in_roundtrip(fts_table):
    """When wand_factor is not set, roundtrip must not introduce it."""
    sync_q = LanceFtsQueryBuilder(fts_table, "machine learning").to_query_object()
    async_tbl = fts_table._table
    async_q = async_tbl._sync_query_to_async(sync_q)

    req = async_q._inner.to_query_request()
    assert getattr(req, "wand_factor", None) is None

    reconstructed = Query.from_inner(req)
    assert reconstructed.full_text_query is not None
    assert reconstructed.full_text_query.wand_factor is None


def test_sync_fts_execution_with_wand_factor(fts_table):
    """Sync FTS query with wand_factor must execute and return results."""
    results = (
        fts_table.search("machine learning", query_type="fts")
        .wand_factor(1.5)
        .limit(10)
        .to_list()
    )
    assert len(results) > 0
    texts = [r["text"] for r in results]
    assert any("machine" in t for t in texts)


def test_sync_fts_default_behavior_unchanged(fts_table):
    """Existing FTS behavior without wand_factor must be unaffected."""
    results_default = (
        fts_table.search("machine learning", query_type="fts").limit(10).to_list()
    )
    results_with_wand = (
        fts_table.search("machine learning", query_type="fts")
        .wand_factor(1.0)
        .limit(10)
        .to_list()
    )
    assert len(results_default) > 0
    assert len(results_with_wand) > 0


@pytest.mark.asyncio
async def test_async_fts_execution_with_wand_factor(async_fts_table):
    """Async FTS query with wand_factor must execute and return results."""
    q = await async_fts_table.search("machine learning", query_type="fts")
    results = await q.wand_factor(1.5).limit(10).to_arrow()
    assert results.num_rows > 0


@pytest.mark.asyncio
async def test_async_fts_wand_factor_stored(async_fts_table):
    """AsyncFTSQuery.wand_factor() must be stored and retrievable."""
    q = await async_fts_table.search("machine learning", query_type="fts")
    assert isinstance(q, AsyncFTSQuery)
    q.wand_factor(2.5)
    assert q.get_wand_factor() == pytest.approx(2.5)


@pytest.mark.asyncio
async def test_async_fts_default_wand_factor_is_none(async_fts_table):
    """AsyncFTSQuery must not have wand_factor set by default."""
    q = await async_fts_table.search("machine learning", query_type="fts")
    assert isinstance(q, AsyncFTSQuery)
    assert q.get_wand_factor() is None


@pytest.mark.asyncio
@pytest.mark.parametrize("value", [-1.0, float("nan"), float("inf"), float("-inf")])
async def test_async_fts_wand_factor_validation(async_fts_table, value):
    q = await async_fts_table.search("machine learning", query_type="fts")
    with pytest.raises(ValueError, match="finite and non-negative"):
        q.wand_factor(value)


def test_hybrid_builder_wand_factor_stored(fts_table):
    """LanceHybridQueryBuilder.wand_factor() must be stored in the builder."""
    builder = LanceHybridQueryBuilder(fts_table, "machine learning")
    builder.wand_factor(1.8)
    assert builder._wand_factor == pytest.approx(1.8)


def test_hybrid_builder_wand_factor_negative_raises(fts_table):
    """Negative wand_factor must raise ValueError on the hybrid builder."""
    builder = LanceHybridQueryBuilder(fts_table, "machine learning")
    with pytest.raises(ValueError, match="non-negative"):
        builder.wand_factor(-0.5)


@pytest.mark.parametrize("value", [float("nan"), float("inf"), float("-inf")])
def test_hybrid_builder_wand_factor_non_finite_raises(fts_table, value):
    builder = LanceHybridQueryBuilder(fts_table, "machine learning")
    with pytest.raises(ValueError, match="finite"):
        builder.wand_factor(value)


def test_hybrid_builder_wand_factor_returns_self(fts_table):
    """wand_factor() must be fluent on LanceHybridQueryBuilder."""
    builder = LanceHybridQueryBuilder(fts_table, "machine learning")
    result = builder.wand_factor(1.0)
    assert result is builder


def test_hybrid_builder_wand_factor_applied_to_fts_child(fts_table):
    """wand_factor must be propagated to the FTS child builder."""
    builder = (
        fts_table.search(query_type="hybrid")
        .vector([0.1, 0.2, 0.3, 0.4])
        .text("machine learning")
        .wand_factor(2.2)
    )
    assert isinstance(builder, LanceHybridQueryBuilder)
    builder._create_query_builders()
    assert builder._fts_query._wand_factor == pytest.approx(2.2)


@pytest.mark.asyncio
async def test_async_hybrid_wand_factor_stored(async_fts_table):
    """AsyncHybridQuery.wand_factor() must be stored and retrievable."""
    q = (
        async_fts_table.query()
        .nearest_to(np.array([0.1, 0.2, 0.3, 0.4], dtype=np.float32))
        .nearest_to_text("machine learning")
    )
    assert isinstance(q, AsyncHybridQuery)
    q.wand_factor(3.1)
    assert q.get_wand_factor() == pytest.approx(3.1)


@pytest.mark.asyncio
async def test_async_hybrid_default_wand_factor_is_none(async_fts_table):
    """AsyncHybridQuery must not have wand_factor set by default."""
    q = (
        async_fts_table.query()
        .nearest_to(np.array([0.1, 0.2, 0.3, 0.4], dtype=np.float32))
        .nearest_to_text("machine learning")
    )
    assert isinstance(q, AsyncHybridQuery)
    assert q.get_wand_factor() is None


@pytest.mark.asyncio
async def test_async_hybrid_wand_factor_conversion_and_clear(async_fts_table):
    q = (
        async_fts_table.query()
        .nearest_to(np.array([1.0, 0.0, 0.0, 0.0], dtype=np.float32))
        .nearest_to_text("machine learning")
    )
    q.wand_factor(2.25)
    assert q._inner.to_query_request().wand_factor == pytest.approx(2.25)
    assert q._inner.to_fts_query().get_wand_factor() == pytest.approx(2.25)
    q.wand_factor(None)
    assert q.get_wand_factor() is None
    assert q._inner.to_query_request().wand_factor is None


def test_sync_hybrid_execution_with_wand_factor(fts_table):
    results = (
        fts_table.search(query_type="hybrid")
        .vector([1.0, 0.0, 0.0, 0.0])
        .text("machine learning")
        .wand_factor(2.25)
        .limit(2)
        .to_list()
    )
    assert len(results) > 0


@pytest.mark.asyncio
async def test_async_hybrid_execution_with_wand_factor(async_fts_table):
    q = (
        async_fts_table.query()
        .nearest_to(np.array([1.0, 0.0, 0.0, 0.0], dtype=np.float32))
        .nearest_to_text("machine learning")
        .wand_factor(2.25)
        .limit(2)
    )
    results = await q.to_arrow()
    assert results.num_rows > 0


def test_requires_local_namespace_execution_with_wand_factor():
    """_requires_local_namespace_execution must return True when wand_factor is set."""
    q_no_wand = Query(
        full_text_query=FullTextSearchQuery(query="hello", wand_factor=None)
    )
    assert not _requires_local_namespace_execution(q_no_wand)

    q_with_wand = Query(
        full_text_query=FullTextSearchQuery(query="hello", wand_factor=1.5)
    )
    assert _requires_local_namespace_execution(q_with_wand)


def test_requires_local_namespace_execution_default_query():
    """Plain query without FTS must not force local execution."""
    q = Query()
    assert not _requires_local_namespace_execution(q)


def test_requires_local_namespace_execution_fts_no_wand():
    """FTS query without wand_factor must not force local execution."""
    q = Query(full_text_query=FullTextSearchQuery(query="hello"))
    assert not _requires_local_namespace_execution(q)


def test_regression_fts_basic(fts_table):
    """Basic FTS query without wand_factor must still work correctly."""
    results = fts_table.search("deep learning", query_type="fts").limit(5).to_list()
    assert len(results) > 0
    texts = [r["text"] for r in results]
    assert any("deep" in t or "learning" in t for t in texts)


def test_regression_fts_query_object_unchanged(fts_table):
    """to_query_object() must not break existing code path (no wand_factor)."""
    builder = LanceFtsQueryBuilder(fts_table, "python")
    q = builder.to_query_object()
    assert q.full_text_query is not None
    assert q.full_text_query.columns is None
    assert q.full_text_query.wand_factor is None


def test_regression_fulltext_search_query_fields():
    """FullTextSearchQuery must still accept and expose existing fields."""
    q = FullTextSearchQuery(query="hello", columns=["text"])
    assert q.query == "hello"
    assert q.columns == ["text"]
    assert q.wand_factor is None


@pytest.mark.parametrize(
    "value",
    [-1.0, float("nan"), float("inf"), float("-inf")],
)
def test_pyo3_fts_wand_factor_validation(fts_table, value):
    sync_q = LanceFtsQueryBuilder(fts_table, "machine learning").to_query_object()
    async_q = fts_table._table._sync_query_to_async(sync_q)

    with pytest.raises(ValueError, match="finite and non-negative"):
        async_q._inner.wand_factor(value)
