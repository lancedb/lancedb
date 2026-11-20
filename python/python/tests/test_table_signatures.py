# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

from datetime import timedelta
import inspect
from unittest.mock import AsyncMock, MagicMock

import lancedb
from lancedb.expr import col
from lancedb.index import BTree, FTS, HnswSq, IvfPq
from lancedb.remote.table import RemoteTable
from lancedb.table import LanceTable, Table
import pyarrow as pa
import pytest


@pytest.mark.parametrize(
    "method",
    [
        "delete",
        "drop_index",
        "index_stats",
        "create_index",
        "create_scalar_index",
        "create_fts_index",
        "search",
        "cleanup_old_versions",
        "optimize_async",
        "optimize",
        "to_polars",
    ],
)
def test_sync_table_signatures(method):
    """A Table call must bind the same arguments on either backend."""

    def parameters(cls):
        return [
            (p.name, p.kind, p.default)
            for p in inspect.signature(getattr(cls, method)).parameters.values()
        ]

    assert parameters(Table) == parameters(LanceTable) == parameters(RemoteTable)


@pytest.fixture(params=[LanceTable, RemoteTable], ids=["local", "remote"])
def sync_table(request, monkeypatch):
    """Exercise real sync wrappers with a recording async backend."""
    handle = MagicMock()
    handle.name = "test"
    handle.create_index = AsyncMock()
    handle.delete = AsyncMock()
    handle.drop_index = AsyncMock()
    handle.index_stats = AsyncMock()
    if request.param is RemoteTable:
        table = RemoteTable(handle, "test_db")
    else:
        table = LanceTable.__new__(LanceTable)
        table._table = handle
        monkeypatch.setattr(table, "_ensure_no_legacy_fts_index", lambda: None)
    return table, handle


def test_sync_keyword_names(sync_table):
    table, handle = sync_table
    predicate = col("id") == 1
    table.delete(where=predicate)
    handle.delete.assert_awaited_once()
    expected = predicate._inner if isinstance(table, LanceTable) else predicate
    assert handle.delete.await_args.args[0] is expected
    table.drop_index(name="vector_idx")
    handle.drop_index.assert_awaited_once_with("vector_idx")
    table.index_stats(index_name="vector_idx")
    handle.index_stats.assert_awaited_once_with("vector_idx")


def test_legacy_vector_positional_arguments(sync_table):
    table, handle = sync_table
    with pytest.warns(DeprecationWarning):
        table.create_index("cosine", 2)
    args, kwargs = handle.create_index.await_args
    assert args == ("vector",)
    assert isinstance(kwargs["config"], IvfPq)
    assert kwargs["config"].distance_type == "cosine"
    assert kwargs["config"].num_partitions == 2
    assert kwargs["replace"] is True


def test_vector_tuning_options(sync_table):
    table, handle = sync_table
    timeout = timedelta(seconds=2)
    with pytest.warns(DeprecationWarning):
        table.create_index(
            index_type="IVF_HNSW_SQ",
            num_partitions=1,
            m=10,
            ef_construction=50,
            max_iterations=12,
            sample_rate=64,
            target_partition_size=1000,
            wait_timeout=timeout,
            name="custom_idx",
            replace=False,
            train=False,
        )
    kwargs = handle.create_index.await_args.kwargs
    config = kwargs.pop("config")
    assert isinstance(config, HnswSq)
    for name, value in {
        "num_partitions": 1,
        "m": 10,
        "ef_construction": 50,
        "max_iterations": 12,
        "sample_rate": 64,
        "target_partition_size": 1000,
    }.items():
        assert getattr(config, name) == value
    assert kwargs == dict(
        wait_timeout=timeout,
        name="custom_idx",
        replace=False,
        train=False,
        exist_ok=False,
    )


def test_scalar_defaults_and_wait_timeout(sync_table):
    table, handle = sync_table
    timeout = timedelta(seconds=2)
    with pytest.warns(DeprecationWarning):
        table.create_scalar_index("id", wait_timeout=timeout)
    kwargs = handle.create_index.await_args.kwargs
    assert isinstance(kwargs["config"], BTree)
    assert kwargs["replace"] is True
    assert kwargs["wait_timeout"] == timeout


def test_fts_tokenizer_and_wait_timeout(sync_table):
    table, handle = sync_table
    timeout = timedelta(seconds=2)
    with pytest.warns(DeprecationWarning):
        table.create_fts_index(
            field_names="text",
            tokenizer_name="en_stem",
            replace=True,
            wait_timeout=timeout,
            custom_stop_words=["example"],
        )
    args, kwargs = handle.create_index.await_args
    assert args == ("text",)
    assert kwargs["replace"] is True
    assert kwargs["wait_timeout"] == timeout
    config = kwargs["config"]
    assert isinstance(config, FTS)
    assert config.language == "English"
    assert config.stem is True
    assert config.custom_stop_words == ["example"]


@pytest.mark.parametrize(
    "kwargs, message",
    [
        ({"use_tantivy": True}, "Tantivy-based FTS has been removed"),
        ({"ordering_field_names": "id"}, "ordering_field_names"),
        ({"writer_heap_size": 123}, "writer_heap_size"),
        ({"tokenizer_name": "invalid"}, "Invalid tokenizer name"),
    ],
)
def test_fts_unsupported_options(sync_table, kwargs, message):
    table, handle = sync_table
    with pytest.warns(DeprecationWarning), pytest.raises(ValueError, match=message):
        table.create_fts_index("text", **kwargs)
    handle.create_index.assert_not_awaited()


@pytest.mark.parametrize(
    "method, kwargs",
    [
        (
            "cleanup_old_versions",
            {"older_than": timedelta(days=1), "delete_unverified": True},
        ),
    ],
)
def test_remote_maintenance_options_warn(method, kwargs):
    table = RemoteTable(MagicMock(name="test"), "test_db")
    with pytest.warns(UserWarning, match="no-op"):
        getattr(table, method)(**kwargs)


def test_remote_optimize_options():
    handle = MagicMock()
    handle._do_optimize = AsyncMock(side_effect=NotImplementedError("remote optimize"))
    table = RemoteTable(handle, "test_db")
    age = timedelta(days=1)
    with pytest.raises(NotImplementedError, match="remote optimize"):
        table.optimize(cleanup_older_than=age, delete_unverified=True, retrain=True)
    handle._do_optimize.assert_awaited_once_with(86_400_000, True, None, None)


@pytest.mark.parametrize(
    "method, old, new, value",
    [
        ("delete", "predicate", "where", "id = 1"),
        ("drop_index", "index_name", "name", "vector_idx"),
        ("index_stats", "index_uuid", "index_name", "vector_idx"),
        ("create_fts_index", "column", "field_names", "text"),
    ],
)
def test_deprecated_remote_keywords(method, old, new, value):
    handle = MagicMock()
    handle.name = "test"
    target = "create_index" if method == "create_fts_index" else method
    setattr(handle, target, AsyncMock())
    table = RemoteTable(handle, "test_db")
    with pytest.warns(DeprecationWarning, match=f"{old} is deprecated"):
        getattr(table, method)(**{old: value})
    assert getattr(handle, target).await_args.args == (value,)
    with pytest.raises(TypeError, match="Cannot specify both"):
        getattr(table, method)(**{old: value, new: value})


@pytest.mark.parametrize("remote_wrapper", [False, True], ids=["local", "remote"])
def test_sync_search_and_polars(tmp_path, remote_wrapper):
    """Use the native engine through both sync wrappers, without a cloud service."""
    local = lancedb.connect(tmp_path).create_table(
        "test",
        [{"id": 1, "text": "quick fox", "vector": [1.0, 1.0]}],
    )
    table = RemoteTable(local._table, "test_db") if remote_wrapper else local
    table.create_index("text", config=FTS())
    assert table.search("quick", query_type="fts").to_arrow()["id"].to_pylist() == [1]
    # Both backends must accept the deprecated ordering argument in the same slot.
    with pytest.warns(DeprecationWarning, match="ordering_field_name"):
        table.search("quick", None, "fts", "id", ["text"])
    fast = table.search([1.0, 1.0], fast_search=True).to_arrow()
    assert len(fast) == 0  # No vector index: fast_search excludes unindexed rows.
    assert table.search([1.0, 1.0]).to_arrow()["id"].to_pylist() == [1]
    if remote_wrapper:
        with pytest.raises(NotImplementedError, match=r"table.search\(\).to_polars"):
            table.to_polars(batch_size=1)
        assert table.search().to_polars()["id"].to_list() == [1]
    else:
        assert table.to_polars(batch_size=1).collect()["id"].to_list() == [1]
    table.delete(where="id = 1")
    assert table.count_rows() == 0


def test_index_replacement_defaults(tmp_path):
    local = lancedb.connect(tmp_path).create_table("test", pa.table({"id": [1]}))
    table = RemoteTable(local._table, "test_db")
    for sync in [local, table]:
        with pytest.warns(DeprecationWarning):
            sync.create_scalar_index("id")
    assert len(list(table.list_indices())) == 1


@pytest.mark.parametrize("cls", [Table, LanceTable, RemoteTable])
def test_optimize_retains_legacy_compaction_aliases(cls):
    assert hasattr(cls, "compact_files")
    assert hasattr(cls, "compact_async")


def test_remote_optimize_rewrite_action():
    handle = MagicMock()
    handle._do_optimize = AsyncMock(return_value="accepted")
    table = RemoteTable(handle, "test_db")
    assert table.optimize(action="rewrite_data") == "accepted"
    handle._do_optimize.assert_awaited_once_with(None, False, None, "rewrite_data")


@pytest.mark.parametrize("cls", [Table, LanceTable, RemoteTable])
@pytest.mark.parametrize("method", ["optimize", "optimize_async"])
def test_optimize_public_options_names(cls, method):
    parameters = inspect.signature(getattr(cls, method)).parameters
    assert parameters["options"].kind is inspect.Parameter.VAR_KEYWORD
    assert "data_options" not in parameters
    assert "compaction_options" not in parameters


def test_optimize_public_types():
    import importlib.util
    from lancedb.optimize import OptimizeResult
    from lancedb.table import OptimizeOptions

    assert lancedb.OptimizeOptions is OptimizeOptions
    assert "retrain" not in OptimizeOptions.__annotations__
    assert lancedb.CompactionOptions is OptimizeOptions
    assert not hasattr(lancedb, "OptimizeDataOptions")
    assert importlib.util.find_spec("lancedb.compaction") is not None
    from lancedb.compaction import CompactionResult

    assert CompactionResult is OptimizeResult
    result = OptimizeResult.from_json(
        '{"partial_commit":false,"rows_succeeded":1,"rows_failed":0,'
        '"completion_percentage":100}'
    )
    assert result.rows_succeeded == 1


def test_flat_optimize_forwarding(sync_table):
    table, handle = sync_table
    handle._do_optimize = AsyncMock(return_value="accepted")
    assert table.optimize(target_rows_per_fragment=100, io_buffer_size=0) == "accepted"
    handle._do_optimize.assert_awaited_once_with(
        None, False, {"target_rows_per_fragment": 100, "io_buffer_size": 0}, None
    )


@pytest.mark.parametrize(
    "options",
    [
        {"compaction_options": {"target_rows_per_fragment": 100, "io_buffer_size": 0}},
    ],
)
def test_legacy_optimize_options_forwarded(sync_table, options):
    table, handle = sync_table
    handle._do_optimize = AsyncMock(return_value="accepted")
    with pytest.warns(DeprecationWarning, match="compaction_options"):
        assert table.optimize(**options) == "accepted"
    handle._do_optimize.assert_awaited_once_with(
        None, False, {"target_rows_per_fragment": 100, "io_buffer_size": 0}, None
    )


def test_legacy_compact_async_forwarded(sync_table):
    table, handle = sync_table
    handle.optimize_async = AsyncMock(return_value=MagicMock())
    with pytest.warns(DeprecationWarning, match="compact_async"):
        table.compact_async(compaction_options={"io_buffer_size": 0})
    handle.optimize_async.assert_awaited_once_with(io_buffer_size=0)


def test_remote_legacy_compact_files_is_still_noop():
    handle = MagicMock()
    table = RemoteTable(handle, "test_db")
    with pytest.warns(UserWarning, match="no-op"):
        assert table.compact_files(target_rows_per_fragment=10) is None
    handle.optimize_async.assert_not_called()
    handle._do_optimize.assert_not_called()
