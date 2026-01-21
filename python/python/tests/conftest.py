# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

from datetime import timedelta

from lancedb.db import AsyncConnection, DBConnection
import lancedb
import pytest
import pytest_asyncio


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
