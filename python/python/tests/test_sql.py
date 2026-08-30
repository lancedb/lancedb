# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

import pytest
import pyarrow as pa

import lancedb
from lancedb import _lancedb
from lancedb.arrow import AsyncRecordBatchReader
from lancedb.sql import AsyncQuery, Query


class FakeNativeQuery:
    id = "query-id"

    async def result(self):
        return pa.table({"value": [1, 2]})


def remote_connection(sql_host_override=None):
    return lancedb.connect(
        "db://analytics",
        api_key="test-key",
        host_override="http://localhost:10024",
        sql_host_override=sql_host_override,
    )


def test_sql_is_connection_scoped():
    assert hasattr(lancedb, "sql")
    assert not callable(lancedb.sql)
    assert not hasattr(_lancedb, "sql")
    assert not hasattr(remote_connection(), "sql")
    assert hasattr(remote_connection(), "submit_query")
    assert hasattr(remote_connection(), "describe_query")


def test_connection_serializes_sql_host_override():
    endpoint = "grpc+tls://sql.example.com:10026"
    restored = lancedb.deserialize_conn(
        remote_connection(sql_host_override=endpoint).serialize()
    )
    assert restored.sql_host_override == endpoint


@pytest.mark.asyncio
async def test_async_sql_result_is_record_batch_stream():
    reader = await AsyncQuery(FakeNativeQuery()).result()
    assert isinstance(reader, AsyncRecordBatchReader)
    assert (await reader.read_all())[0].column(0).to_pylist() == [1, 2]


def test_sync_sql_result_is_record_batch_reader():
    reader = Query(AsyncQuery(FakeNativeQuery())).result()
    assert isinstance(reader, pa.RecordBatchReader)
    assert reader.read_all().column(0).to_pylist() == [1, 2]


def test_local_connection_rejects_sql(tmp_path):
    connection = lancedb.connect(tmp_path)
    with pytest.raises(NotImplementedError, match="SQL"):
        connection.submit_query("SELECT 1")
    with pytest.raises(NotImplementedError, match="SQL"):
        connection.describe_query("query-id")


@pytest.mark.asyncio
async def test_local_async_connection_rejects_sql(tmp_path):
    connection = await lancedb.connect_async(tmp_path)
    with pytest.raises(NotImplementedError, match="SQL"):
        await connection.submit_query("SELECT 1")
    with pytest.raises(NotImplementedError, match="SQL"):
        await connection.describe_query("query-id")


@pytest.mark.asyncio
async def test_async_namespace_connection_rejects_sql(tmp_path):
    connection = lancedb.connect_namespace_async("dir", {"root": str(tmp_path)})
    with pytest.raises(NotImplementedError, match="SQL"):
        await connection.submit_query("SELECT 1")
    with pytest.raises(NotImplementedError, match="SQL"):
        await connection.describe_query("query-id")


@pytest.mark.parametrize(
    "default_namespace_path",
    ["public", ("public",), [1]],
)
def test_submit_query_requires_namespace_path_list(default_namespace_path):
    with pytest.raises(ValueError, match="default_namespace_path"):
        remote_connection().submit_query(
            "SELECT 1", default_namespace_path=default_namespace_path
        )


def test_submit_query_rejects_invalid_endpoint():
    connection = remote_connection(sql_host_override="invalid://localhost")
    with pytest.raises(ValueError, match="sql_host_override"):
        connection.submit_query("SELECT 1")


@pytest.mark.parametrize(
    "default_namespace_path",
    [[""], ["café"], ["pub\tlic"], ["events$raw"]],
)
def test_submit_query_rejects_invalid_namespace_components(default_namespace_path):
    with pytest.raises(ValueError, match="default_namespace_path"):
        remote_connection().submit_query(
            "SELECT 1", default_namespace_path=default_namespace_path
        )
