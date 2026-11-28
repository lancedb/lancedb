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


def test_fts_builder_set_wand_factor_1_0(fts_table):
    """wand_factor=1.0 set on LanceFtsQueryBuilder is stored in the Query object."""
    builder = LanceFtsQueryBuilder(fts_table, "machine learning").wand_factor(1.0)
    query_obj = builder.to_query_object()
    assert query_obj.full_text_query is not None
    assert query_obj.full_text_query.wand_factor == pytest.approx(1.0)


def test_fts_builder_wand_factor_negative_raises(fts_table):
    """Negative wand_factor must raise ValueError."""
    builder = LanceFtsQueryBuilder(fts_table, "machine learning")
    with pytest.raises(ValueError, match="finite and non-negative"):
        builder.wand_factor(-1.0)


@pytest.mark.parametrize("value", [float("nan"), float("inf"), float("-inf")])
def test_fts_builder_wand_factor_non_finite_raises(fts_table, value):
    builder = LanceFtsQueryBuilder(fts_table, "machine learning")
    with pytest.raises(ValueError, match="finite and non-negative"):
        builder.wand_factor(value)


@pytest.mark.parametrize("value", [0.0, 0.8, 1.5, 2.0])
def test_fts_builder_unsupported_factor_raises(fts_table, value):
    """Non-default factors other than 1.0 must be rejected (Lance issue #11206)."""
    builder = LanceFtsQueryBuilder(fts_table, "machine learning")
    with pytest.raises(ValueError, match="must be None or 1.0"):
        builder.wand_factor(value)


@pytest.mark.parametrize("value", [0.0, 0.8, 1.5, 2.0])
def test_full_text_search_query_unsupported_factor_raises(value):
    """FullTextSearchQuery must reject non-default factors other than 1.0."""
    with pytest.raises(ValueError, match="must be None or 1.0"):
        FullTextSearchQuery(query="hello", wand_factor=value)


def test_full_text_search_query_valid_factors():
    """FullTextSearchQuery must accept None and 1.0."""
    q_none = FullTextSearchQuery(query="hello", wand_factor=None)
    assert q_none.wand_factor is None
    q_one = FullTextSearchQuery(query="hello", wand_factor=1.0)
    assert q_one.wand_factor == pytest.approx(1.0)


def test_fts_builder_wand_factor_returns_self(fts_table):
    """wand_factor() must be fluent (returns the builder)."""
    builder = LanceFtsQueryBuilder(fts_table, "machine learning")
    result = builder.wand_factor(1.0)
    assert result is builder


def test_fts_builder_wand_factor_none_clears(fts_table):
    """Calling wand_factor(None) should clear the value."""
    builder = (
        LanceFtsQueryBuilder(fts_table, "machine learning")
        .wand_factor(1.0)
        .wand_factor(None)
    )
    query_obj = builder.to_query_object()
    assert query_obj.full_text_query.wand_factor is None


def test_pyo3_fts_query_wand_factor(fts_table):
    """FTSQuery (PyO3) getter/setter round-trip works correctly."""
    sync_q = (
        LanceFtsQueryBuilder(fts_table, "machine learning")
        .wand_factor(1.0)
        .to_query_object()
    )
    async_tbl = fts_table._table
    async_q = async_tbl._sync_query_to_async(sync_q)
    assert isinstance(async_q, AsyncFTSQuery)
    assert async_q.get_wand_factor() == pytest.approx(1.0)


def test_pyo3_fts_query_wand_factor_roundtrip(fts_table):
    """wand_factor survives PyO3 FTSQuery getter/setter round-trip."""
    sync_q = (
        LanceFtsQueryBuilder(fts_table, "machine learning")
        .wand_factor(1.0)
        .to_query_object()
    )
    async_tbl = fts_table._table
    async_q = async_tbl._sync_query_to_async(sync_q)
    assert isinstance(async_q, AsyncFTSQuery)

    req = async_q._inner.to_query_request()
    assert hasattr(req, "wand_factor"), "PyQueryRequest is missing wand_factor field"
    assert req.wand_factor == pytest.approx(1.0)


def test_pyo3_hybrid_query_wand_factor(fts_table):
    """HybridQuery (PyO3) delegates wand_factor to its inner FTSQuery."""
    builder = (
        fts_table.search(query_type="hybrid")
        .vector([0.1, 0.2, 0.3, 0.4])
        .text("machine learning")
    )
    assert isinstance(builder, LanceHybridQueryBuilder)
    builder.wand_factor(1.0)
    assert builder._wand_factor == pytest.approx(1.0)
    builder._create_query_builders()
    fts_child = builder._fts_query
    assert isinstance(fts_child, LanceFtsQueryBuilder)
    assert fts_child._wand_factor == pytest.approx(1.0)


def test_wand_factor_roundtrip_via_query_request(fts_table):
    """wand_factor must survive a PyQueryRequest -> Query.from_inner() roundtrip."""
    sync_q = (
        LanceFtsQueryBuilder(fts_table, "machine learning")
        .wand_factor(1.0)
        .to_query_object()
    )
    async_tbl = fts_table._table
    async_q = async_tbl._sync_query_to_async(sync_q)

    req = async_q._inner.to_query_request()
    assert hasattr(req, "wand_factor"), "PyQueryRequest is missing wand_factor field"
    assert req.wand_factor == pytest.approx(1.0)

    reconstructed = Query.from_inner(req)
    assert reconstructed.full_text_query is not None
    assert reconstructed.full_text_query.wand_factor == pytest.approx(1.0)


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
    """Sync FTS query with wand_factor=1.0 must execute and return results."""
    results = (
        fts_table.search("machine learning", query_type="fts")
        .wand_factor(1.0)
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
    """Async FTS query with wand_factor=1.0 must execute and return results."""
    q = await async_fts_table.search("machine learning", query_type="fts")
    results = await q.wand_factor(1.0).limit(10).to_arrow()
    assert results.num_rows > 0


@pytest.mark.asyncio
async def test_async_fts_wand_factor_stored(async_fts_table):
    """AsyncFTSQuery.wand_factor() must be stored and retrievable."""
    q = await async_fts_table.search("machine learning", query_type="fts")
    assert isinstance(q, AsyncFTSQuery)
    q.wand_factor(1.0)
    assert q.get_wand_factor() == pytest.approx(1.0)


@pytest.mark.asyncio
async def test_async_fts_default_wand_factor_is_none(async_fts_table):
    """AsyncFTSQuery must not have wand_factor set by default."""
    q = await async_fts_table.search("machine learning", query_type="fts")
    assert isinstance(q, AsyncFTSQuery)
    assert q.get_wand_factor() is None


@pytest.mark.asyncio
@pytest.mark.parametrize("value", [-1.0, float("nan"), float("inf"), float("-inf")])
async def test_async_fts_wand_factor_validation_non_finite(async_fts_table, value):
    q = await async_fts_table.search("machine learning", query_type="fts")
    with pytest.raises(ValueError, match="finite and non-negative"):
        q.wand_factor(value)


@pytest.mark.asyncio
@pytest.mark.parametrize("value", [0.0, 0.8, 1.5, 2.0])
async def test_async_fts_wand_factor_unsupported_raises(async_fts_table, value):
    q = await async_fts_table.search("machine learning", query_type="fts")
    with pytest.raises(ValueError, match="must be None or 1.0"):
        q.wand_factor(value)


def test_hybrid_builder_wand_factor_stored(fts_table):
    """LanceHybridQueryBuilder.wand_factor() must be stored in the builder."""
    builder = LanceHybridQueryBuilder(fts_table, "machine learning")
    builder.wand_factor(1.0)
    assert builder._wand_factor == pytest.approx(1.0)


def test_hybrid_builder_wand_factor_negative_raises(fts_table):
    """Negative wand_factor must raise ValueError on the hybrid builder."""
    builder = LanceHybridQueryBuilder(fts_table, "machine learning")
    with pytest.raises(ValueError, match="finite and non-negative"):
        builder.wand_factor(-0.5)


@pytest.mark.parametrize("value", [float("nan"), float("inf"), float("-inf")])
def test_hybrid_builder_wand_factor_non_finite_raises(fts_table, value):
    builder = LanceHybridQueryBuilder(fts_table, "machine learning")
    with pytest.raises(ValueError, match="finite and non-negative"):
        builder.wand_factor(value)


@pytest.mark.parametrize("value", [0.0, 0.8, 1.5, 2.0])
def test_hybrid_builder_wand_factor_unsupported_raises(fts_table, value):
    builder = LanceHybridQueryBuilder(fts_table, "machine learning")
    with pytest.raises(ValueError, match="must be None or 1.0"):
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
        .wand_factor(1.0)
    )
    assert isinstance(builder, LanceHybridQueryBuilder)
    builder._create_query_builders()
    assert builder._fts_query._wand_factor == pytest.approx(1.0)


@pytest.mark.asyncio
async def test_async_hybrid_wand_factor_stored(async_fts_table):
    """AsyncHybridQuery.wand_factor() must be stored and retrievable."""
    q = (
        async_fts_table.query()
        .nearest_to(np.array([0.1, 0.2, 0.3, 0.4], dtype=np.float32))
        .nearest_to_text("machine learning")
    )
    assert isinstance(q, AsyncHybridQuery)
    q.wand_factor(1.0)
    assert q.get_wand_factor() == pytest.approx(1.0)


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
@pytest.mark.parametrize("value", [0.0, 0.8, 1.5, 2.0])
async def test_async_hybrid_wand_factor_unsupported_raises(async_fts_table, value):
    q = (
        async_fts_table.query()
        .nearest_to(np.array([0.1, 0.2, 0.3, 0.4], dtype=np.float32))
        .nearest_to_text("machine learning")
    )
    with pytest.raises(ValueError, match="must be None or 1.0"):
        q.wand_factor(value)


@pytest.mark.asyncio
async def test_async_hybrid_wand_factor_conversion_and_clear(async_fts_table):
    q = (
        async_fts_table.query()
        .nearest_to(np.array([1.0, 0.0, 0.0, 0.0], dtype=np.float32))
        .nearest_to_text("machine learning")
    )
    q.wand_factor(1.0)
    assert q._inner.to_query_request().wand_factor == pytest.approx(1.0)
    assert q._inner.to_fts_query().get_wand_factor() == pytest.approx(1.0)
    q.wand_factor(None)
    assert q.get_wand_factor() is None
    assert q._inner.to_query_request().wand_factor is None


def test_sync_hybrid_execution_with_wand_factor(fts_table):
    results = (
        fts_table.search(query_type="hybrid")
        .vector([1.0, 0.0, 0.0, 0.0])
        .text("machine learning")
        .wand_factor(1.0)
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
        .wand_factor(1.0)
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
        full_text_query=FullTextSearchQuery(query="hello", wand_factor=1.0)
    )
    assert _requires_local_namespace_execution(q_with_wand)


def test_requires_local_namespace_execution_wand_factor_1_0():
    """wand_factor=1.0 (non-None, explicit) must also require local execution."""
    q = Query(full_text_query=FullTextSearchQuery(query="hello", wand_factor=1.0))
    assert _requires_local_namespace_execution(q)


def test_requires_local_namespace_execution_default_query():
    """Plain query without FTS must not force local execution."""
    q = Query()
    assert not _requires_local_namespace_execution(q)


def test_requires_local_namespace_execution_fts_no_wand():
    """FTS query without wand_factor must not force local execution."""
    q = Query(full_text_query=FullTextSearchQuery(query="hello"))
    assert not _requires_local_namespace_execution(q)


def test_requires_local_namespace_execution_force_local_flag():
    """_force_local_namespace private attr routes any query locally."""
    q = Query(vector=[1.0, 0.0, 0.0, 0.0])
    assert not _requires_local_namespace_execution(q)
    q._force_local_namespace = True
    assert _requires_local_namespace_execution(q)


def test_hybrid_wand_vector_leg_marked_local(fts_table):
    """When a hybrid query has wand_factor, the vector sub-query must carry
    _force_local_namespace=True so that _requires_local_namespace_execution
    routes it to the same embedded table snapshot as the FTS leg."""
    builder = (
        fts_table.search(query_type="hybrid")
        .vector([0.1, 0.2, 0.3, 0.4])
        .text("machine learning")
        .wand_factor(1.0)
    )
    assert isinstance(builder, LanceHybridQueryBuilder)
    builder._create_query_builders()

    # FTS leg: wand_factor is set -> requires local
    fts_q_obj = builder._fts_query.to_query_object()
    assert fts_q_obj.full_text_query.wand_factor == pytest.approx(1.0)
    assert _requires_local_namespace_execution(fts_q_obj)

    # Vector leg: _force_local_namespace must be True
    vec_q_obj = builder._vector_query.to_query_object()
    assert vec_q_obj._force_local_namespace is True
    assert _requires_local_namespace_execution(vec_q_obj)


def test_hybrid_no_wand_vector_leg_not_forced_local(fts_table):
    """Without wand_factor the vector sub-query must NOT carry the local flag."""
    builder = (
        fts_table.search(query_type="hybrid")
        .vector([0.1, 0.2, 0.3, 0.4])
        .text("machine learning")
    )
    builder._create_query_builders()

    vec_q_obj = builder._vector_query.to_query_object()
    assert vec_q_obj._force_local_namespace is False
    assert not _requires_local_namespace_execution(vec_q_obj)


@pytest.mark.asyncio
async def test_async_hybrid_wand_vector_leg_marked_local(tmp_path):
    """Async: when a hybrid query has wand_factor, the vector sub-query must carry
    _force_local_namespace=True so that both legs execute on the same local
    embedded table snapshot."""
    properties = {"root": str(tmp_path)}
    db = lancedb.connect_namespace_async("dir", properties)
    tbl = await db.create_table(
        "async_leg_test",
        data=pa.table(
            {
                "id": [1],
                "vector": [[1.0, 0.0, 0.0, 0.0]],
                "text": ["hello world"],
            }
        ),
    )
    await tbl.create_index("text", config=lancedb.index.FTS())
    await tbl.create_index("vector")

    # With wand_factor(1.0)
    q_wand = (
        tbl.query()
        .nearest_to(np.array([1.0, 0.0, 0.0, 0.0], dtype=np.float32))
        .nearest_to_text("hello")
        .wand_factor(1.0)
    )
    _, vec_child, _, _ = await q_wand._create_child_queries()
    assert vec_child._force_local_namespace is True

    # Without wand_factor
    q_default = (
        tbl.query()
        .nearest_to(np.array([1.0, 0.0, 0.0, 0.0], dtype=np.float32))
        .nearest_to_text("hello")
    )
    _, vec_child_default, _, _ = await q_default._create_child_queries()
    assert vec_child_default._force_local_namespace is False


def test_namespace_hybrid_deleted_row_snapshot_consistency(tmp_path):
    """Integration test: deleting a row on a namespace table with QueryTable pushdown
    must return identical results for hybrid queries with or without wand_factor(1.0),
    and neither query must ever return the deleted row."""
    properties = {"root": str(tmp_path)}
    db = lancedb.connect_namespace(
        "dir", properties, namespace_client_pushdown_operations=["QueryTable"]
    )
    data = pa.table(
        {
            "id": [1, 2, 3],
            "text": ["apple fruit", "banana fruit", "apple pie"],
            "vector": [
                [1.0, 0.0, 0.0, 0.0],
                [0.0, 1.0, 0.0, 0.0],
                [0.5, 0.5, 0.0, 0.0],
            ],
        }
    )
    tbl = db.create_table("ns_hybrid_tbl", data=data)
    tbl.create_index("text", config=lancedb.index.FTS())
    tbl.create_index("vector")

    # Delete matching row 1
    tbl.delete("id = 1")

    # Hybrid query without wand_factor
    res_default = (
        tbl.search(query_type="hybrid")
        .vector([1.0, 0.0, 0.0, 0.0])
        .text("apple")
        .limit(10)
        .to_list()
    )
    ids_default = [r["id"] for r in res_default]

    # Hybrid query with wand_factor(1.0)
    res_wand = (
        tbl.search(query_type="hybrid")
        .vector([1.0, 0.0, 0.0, 0.0])
        .text("apple")
        .wand_factor(1.0)
        .limit(10)
        .to_list()
    )
    ids_wand = [r["id"] for r in res_wand]

    assert 1 not in ids_default, "Deleted row returned in default hybrid query"
    assert 1 not in ids_wand, "Deleted row returned in wand_factor(1.0) hybrid query"
    assert ids_default == ids_wand, (
        f"Hybrid results diverged: default={ids_default}, wand={ids_wand}"
    )


@pytest.mark.asyncio
async def test_async_namespace_hybrid_deleted_row_snapshot_consistency(tmp_path):
    """Async integration test: deleting a row on a namespace table
    with QueryTable pushdown must return identical results for hybrid
    queries with or without wand_factor(1.0), and neither query must ever
    return the deleted row."""
    properties = {"root": str(tmp_path)}
    db = lancedb.connect_namespace_async(
        "dir", properties, namespace_client_pushdown_operations=["QueryTable"]
    )
    data = pa.table(
        {
            "id": [1, 2, 3],
            "text": ["apple fruit", "banana fruit", "apple pie"],
            "vector": [
                [1.0, 0.0, 0.0, 0.0],
                [0.0, 1.0, 0.0, 0.0],
                [0.5, 0.5, 0.0, 0.0],
            ],
        }
    )
    tbl = await db.create_table("async_ns_hybrid_tbl", data=data)
    assert "QueryTable" in tbl._pushdown_operations
    await tbl.create_index("text", config=lancedb.index.FTS())
    await tbl.create_index("vector")

    # Instrument tbl._execute_arrow to verify execution routing
    executed_queries = []
    original_execute_arrow = tbl._execute_arrow

    async def instrumented_execute_arrow(query, timeout=None):
        executed_queries.append(query)
        return await original_execute_arrow(query, timeout=timeout)

    tbl._execute_arrow = instrumented_execute_arrow

    # Delete matching row 1
    await tbl.delete("id = 1")

    # Hybrid query without wand_factor
    q_default = (
        tbl.query()
        .nearest_to(np.array([1.0, 0.0, 0.0, 0.0], dtype=np.float32))
        .nearest_to_text("apple")
        .limit(10)
    )
    _, vec_def, _, _ = await q_default._create_child_queries()
    assert vec_def._force_local_namespace is False

    res_default = (await q_default.to_arrow()).to_pydict()["id"]
    # Default path must not have been routed through _execute_arrow forced local
    assert len(executed_queries) == 0

    # Hybrid query with wand_factor(1.0)
    q_wand = (
        tbl.query()
        .nearest_to(np.array([1.0, 0.0, 0.0, 0.0], dtype=np.float32))
        .nearest_to_text("apple")
        .wand_factor(1.0)
        .limit(10)
    )
    _, vec_wand, _, _ = await q_wand._create_child_queries()
    assert vec_wand._force_local_namespace is True

    res_wand = (await q_wand.to_arrow()).to_pydict()["id"]
    # Explicit WAND must have routed the vector leg through _execute_arrow
    assert len(executed_queries) == 1
    assert executed_queries[0]._force_local_namespace is True
    assert _requires_local_namespace_execution(executed_queries[0]) is True

    assert 1 not in res_default, "Deleted row returned in async default hybrid query"
    assert 1 not in res_wand, (
        "Deleted row returned in async wand_factor(1.0) hybrid query"
    )
    assert res_default == res_wand, (
        f"Async hybrid results diverged: default={res_default}, wand={res_wand}"
    )


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
def test_pyo3_fts_wand_factor_validation_non_finite(fts_table, value):
    sync_q = LanceFtsQueryBuilder(fts_table, "machine learning").to_query_object()
    async_q = fts_table._table._sync_query_to_async(sync_q)

    with pytest.raises(ValueError, match="finite and non-negative"):
        async_q._inner.wand_factor(value)


@pytest.mark.parametrize(
    "value",
    [0.0, 0.8, 1.5, 2.0],
)
def test_pyo3_fts_wand_factor_validation_unsupported(fts_table, value):
    """PyO3 direct setter must reject unsupported non-default factors."""
    sync_q = LanceFtsQueryBuilder(fts_table, "machine learning").to_query_object()
    async_q = fts_table._table._sync_query_to_async(sync_q)

    with pytest.raises(ValueError, match="must be None or 1.0"):
        async_q._inner.wand_factor(value)


def test_fulltext_search_query_validate_assignment():
    """validate_assignment=True must enforce wand_factor constraints on mutation."""
    q = FullTextSearchQuery(query="test", wand_factor=1.0)
    assert q.wand_factor == 1.0

    q.wand_factor = None
    assert q.wand_factor is None

    q.wand_factor = 1.0
    assert q.wand_factor == 1.0

    with pytest.raises(ValueError, match="wand_factor must be None or 1.0"):
        q.wand_factor = 1.5

    with pytest.raises(ValueError, match="wand_factor must be None or 1.0"):
        q.wand_factor = 0.8

    with pytest.raises(ValueError, match="wand_factor must be finite and non-negative"):
        q.wand_factor = -1.0


def test_fts_count_rows_preserves_wand_factor_sync(fts_table):
    """An FTS query configured with wand_factor(1.0) must retain that value

    on the original query after count_rows() is called, and count results must
    be unaffected by limit and offset.
    """
    builder = (
        fts_table.search("machine learning", query_type="fts")
        .wand_factor(1.0)
        .limit(1)
        .offset(1)
    )
    assert builder._wand_factor == 1.0
    query_obj_before = builder.to_query_object()
    assert query_obj_before.full_text_query.wand_factor == 1.0

    # count_rows() must count all matches without being capped by limit/offset
    # or WAND early-termination thresholds.
    count = builder.count_rows()
    assert count == 2

    # Original query builder and its to_query_object() must retain wand_factor(1.0)
    assert builder._wand_factor == 1.0
    query_obj_after = builder.to_query_object()
    assert query_obj_after.full_text_query.wand_factor == 1.0

    # Subsequent execution of the original query still respects limit/offset
    results = builder.to_list()
    assert len(results) == 1


@pytest.mark.asyncio
async def test_fts_count_rows_preserves_wand_factor_async(async_fts_table):
    """Async FTS query configured with wand_factor(1.0) must retain that value

    on the original query and PyO3 inner handle after count_rows() is called.
    """
    query = await async_fts_table.search("machine learning", query_type="fts")
    query.wand_factor(1.0).limit(1).offset(1)

    assert query.get_wand_factor() == 1.0
    assert query._inner.get_wand_factor() == 1.0

    count = await query.count_rows()
    assert count == 2

    # Both AsyncFTSQuery wrapper and PyO3 inner must retain wand_factor(1.0)
    assert query.get_wand_factor() == 1.0
    assert query._inner.get_wand_factor() == 1.0

    # Subsequent retrieval execution still respects limit/offset
    results = await query.to_list()
    assert len(results) == 1

    # Also verify query().nearest_to_text(...) builder
    direct = (
        async_fts_table.query()
        .nearest_to_text("machine learning")
        .wand_factor(1.0)
        .limit(1)
        .offset(1)
    )
    assert direct.get_wand_factor() == 1.0
    assert direct._inner.get_wand_factor() == 1.0

    direct_count = await direct.count_rows()
    assert direct_count == 2
    assert direct.get_wand_factor() == 1.0
    assert direct._inner.get_wand_factor() == 1.0


@pytest.mark.asyncio
async def test_fts_count_rows_internal_count_uses_wand_factor_none(
    fts_table, async_fts_table
):
    """The internal count query must use wand_factor=None independently of

    the original retrieval query, returning identical exact counts whether
    wand_factor is None or explicitly set to 1.0.
    """
    # Sync comparison
    sync_default = fts_table.search("machine learning", query_type="fts")
    sync_wand = fts_table.search("machine learning", query_type="fts").wand_factor(1.0)

    count_default = sync_default.count_rows()
    count_wand = sync_wand.count_rows()
    assert count_default == 2
    assert count_wand == count_default

    # Verify PyO3 inner query: count_rows() uses a clone with wand_factor=None,
    # so calling count_rows() leaves the inner object's wand_factor as 1.0.
    sync_q = sync_wand.to_query_object()
    async_q = fts_table._table._sync_query_to_async(sync_q)
    assert async_q._inner.get_wand_factor() == 1.0
    inner_count = await async_q._inner.count_rows()
    assert inner_count == 2
    assert async_q._inner.get_wand_factor() == 1.0

    # Async comparison
    async_default = await async_fts_table.search("machine learning", query_type="fts")
    async_wand = await async_fts_table.search("machine learning", query_type="fts")
    async_wand.wand_factor(1.0)

    assert await async_default.count_rows() == 2
    assert await async_wand.count_rows() == 2


def test_fts_count_rows_unaffected_by_limit_and_offset(fts_table):
    """Count results must be unaffected by the original query's limit and offset;

    counting must cover all matching rows.
    """
    base_builder = fts_table.search("rocks", query_type="fts")
    total_matching = base_builder.count_rows()
    assert total_matching == 2

    # Various limit and offset combinations with and without wand_factor(1.0)
    combinations = [
        {"limit": 0},
        {"limit": 1},
        {"offset": 1},
        {"limit": 1, "offset": 1},
        {"limit": 1, "offset": 10},
    ]

    for params in combinations:
        # Default query (no WAND)
        b = fts_table.search("rocks", query_type="fts")
        if "limit" in params:
            b.limit(params["limit"])
        if "offset" in params:
            b.offset(params["offset"])
        assert b.count_rows() == total_matching

        # Explicit WAND
        b_wand = fts_table.search("rocks", query_type="fts").wand_factor(1.0)
        if "limit" in params:
            b_wand.limit(params["limit"])
        if "offset" in params:
            b_wand.offset(params["offset"])
        assert b_wand.count_rows() == total_matching

    # Where clause filter still applies to count
    filtered = (
        fts_table.search("rocks", query_type="fts")
        .wand_factor(1.0)
        .where("id = 2")
        .limit(1)
        .offset(5)
    )
    assert filtered.count_rows() == 1

    # Absent token returns 0
    absent = (
        fts_table.search("nonexistent_term_xyz", query_type="fts")
        .wand_factor(1.0)
        .limit(1)
    )
    assert absent.count_rows() == 0


@pytest.mark.asyncio
async def test_fts_count_rows_ordinary_query_exact_count(fts_table, async_fts_table):
    """Existing exact-count behavior remains correct for ordinary FTS queries

    without an explicit WAND factor across sync and async APIs.
    """
    # Sync ordinary query
    sync_q = fts_table.search("rocks", query_type="fts")
    assert sync_q._wand_factor is None
    assert sync_q.to_query_object().full_text_query.wand_factor is None
    assert sync_q.count_rows() == 2

    # Preserves query options across count_rows
    sync_q.limit(1).offset(1).where("id >= 2")
    before_obj = sync_q.to_query_object()
    assert sync_q.count_rows() == 2
    assert sync_q.to_query_object() == before_obj
    assert len(sync_q.to_list()) == 1

    # Async ordinary query
    async_q = await async_fts_table.search("rocks", query_type="fts")
    assert async_q.get_wand_factor() is None
    assert async_q._inner.get_wand_factor() is None
    assert await async_q.count_rows() == 2

    async_q.limit(1).offset(1).where("id >= 2")
    assert await async_q.count_rows() == 2
    assert len(await async_q.to_list()) == 1
