# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

import pytest

import lancedb
from lancedb import _lancedb


def remote_connection(sql_host_override=None):
    return lancedb.connect(
        "db://analytics",
        api_key="test-key",
        host_override="http://localhost:10024",
        sql_host_override=sql_host_override,
    )


def test_sql_is_connection_scoped():
    assert not hasattr(lancedb, "sql")
    assert not hasattr(_lancedb, "sql")
    assert hasattr(remote_connection(), "sql")


def test_connection_serializes_sql_host_override():
    endpoint = "grpc+tls://sql.example.com:10026"
    restored = lancedb.deserialize_conn(
        remote_connection(sql_host_override=endpoint).serialize()
    )
    assert restored.sql_host_override == endpoint


def test_local_connection_rejects_sql(tmp_path):
    connection = lancedb.connect(tmp_path)
    with pytest.raises(NotImplementedError, match="Flight SQL"):
        connection.sql("SELECT 1")


@pytest.mark.asyncio
async def test_local_async_connection_rejects_sql(tmp_path):
    connection = await lancedb.connect_async(tmp_path)
    with pytest.raises(NotImplementedError, match="Flight SQL"):
        await connection.sql("SELECT 1")


@pytest.mark.parametrize(
    "default_namespace_path",
    ["public", ("public",), [1]],
)
def test_sql_requires_namespace_path_list(default_namespace_path):
    with pytest.raises(ValueError, match="default_namespace_path"):
        remote_connection().sql(
            "SELECT 1", default_namespace_path=default_namespace_path
        )


def test_sql_rejects_invalid_endpoint():
    connection = remote_connection(sql_host_override="invalid://localhost")
    with pytest.raises(ValueError, match="sql_host_override"):
        connection.sql("SELECT 1")


@pytest.mark.parametrize(
    "default_namespace_path",
    [[""], ["café"], ["pub\tlic"], ["events$raw"]],
)
def test_sql_rejects_invalid_namespace_components(default_namespace_path):
    with pytest.raises(ValueError, match="default_namespace_path"):
        remote_connection().sql(
            "SELECT 1", default_namespace_path=default_namespace_path
        )
