# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

import pytest

import lancedb
from lancedb import _lancedb


def remote_connection():
    return lancedb.connect(
        "db://analytics",
        api_key="test-key",
        host_override="http://localhost:10024",
    )


def test_sql_is_connection_scoped():
    assert not hasattr(lancedb, "sql")
    assert not hasattr(_lancedb, "sql")
    assert hasattr(remote_connection(), "sql")


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
    with pytest.raises(ValueError, match="flight_sql_uri"):
        remote_connection().sql(
            "SELECT 1",
            flight_sql_uri="invalid://localhost",
        )


@pytest.mark.parametrize(
    "default_namespace_path",
    [[""], ["café"], ["pub\tlic"], ["events$raw"]],
)
def test_sql_rejects_invalid_namespace_components(default_namespace_path):
    with pytest.raises(ValueError, match="default_namespace_path"):
        remote_connection().sql(
            "SELECT 1",
            default_namespace_path=default_namespace_path,
            flight_sql_uri="invalid://localhost",
        )
