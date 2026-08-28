# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

import pytest

import lancedb
from lancedb import _lancedb


def test_sql_is_native_binding():
    assert lancedb.sql is _lancedb.sql
    assert lancedb.sql.__module__ == "lancedb._lancedb"


@pytest.mark.parametrize(
    "default_database",
    ["", "db://analytics", "analytics/tenant1", "user@analytics", "analytics:1234"],
)
def test_sql_rejects_invalid_database_name(default_database):
    with pytest.raises(ValueError, match="default_database"):
        lancedb.sql("SELECT 1", default_database=default_database)


@pytest.mark.parametrize(
    "default_namespace_path",
    ["public", ("public",), [1]],
)
def test_sql_requires_namespace_path_list(default_namespace_path):
    with pytest.raises(ValueError, match="default_namespace_path"):
        lancedb.sql("SELECT 1", default_namespace_path=default_namespace_path)


@pytest.mark.parametrize(
    "default_namespace_path",
    [[""], ["café"], ["pub\tlic"], ["events$raw"]],
)
def test_sql_rejects_invalid_namespace_components(default_namespace_path):
    with pytest.raises(ValueError, match="default_namespace_path"):
        lancedb.sql(
            "SELECT 1",
            default_namespace_path=default_namespace_path,
            api_key="test-key",
            host_override="http://localhost:10024",
            flight_sql_uri="invalid://localhost",
        )
