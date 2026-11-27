# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

from datetime import timedelta

from lancedb.db import AsyncConnection, DBConnection
import lancedb
import pytest
import pytest_asyncio


def pytest_configure(config):
    config.addinivalue_line(
        "markers", "experimental_functions: requires the experimental-functions build"
    )


def pytest_collection_modifyitems(items):
    if lancedb._lancedb._experimental_functions_enabled:
        return
    skip = pytest.mark.skip(reason="requires the experimental-functions Cargo feature")
    for item in items:
        if item.get_closest_marker("experimental_functions"):
            item.add_marker(skip)


def pandas_string_type():
    """Return the PyArrow string type that pandas uses for string columns.

    pandas 3.0+ uses large_string for string columns, pandas 2.x uses string.
    """
    import pandas as pd
    import pyarrow as pa

    version = tuple(int(x) for x in pd.__version__.split(".")[:2])
    if version >= (3, 0):
        return pa.large_utf8()
    return pa.utf8()


# Use an in-memory database for most tests.
@pytest.fixture
def mem_db() -> DBConnection:
    return lancedb.connect("memory://")


# Use a temporary directory when we need to inspect the database files.
@pytest.fixture
def tmp_db(tmp_path) -> DBConnection:
    return lancedb.connect(tmp_path)


@pytest_asyncio.fixture
async def mem_db_async() -> AsyncConnection:
    return await lancedb.connect_async("memory://")


@pytest_asyncio.fixture
async def tmp_db_async(tmp_path) -> AsyncConnection:
    return await lancedb.connect_async(
        tmp_path, read_consistency_interval=timedelta(seconds=0)
    )
