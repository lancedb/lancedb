# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

import pytest
from lancedb.db import AsyncConnection


@pytest.mark.asyncio
@pytest.mark.parametrize("operation", ["count_rows", "query", "head"])
async def test_closed_table_queries_raise_runtime_error(
    mem_db_async: AsyncConnection, operation
):
    table = await mem_db_async.create_table("closed_query", data=[{"id": 0}])
    assert (await table.query().to_arrow()).to_pylist() == [{"id": 0}]
    table.close()
    table.close()
    assert not table.is_open()

    with pytest.raises(RuntimeError, match="Table closed_query is closed"):
        if operation == "query":
            table.query()
        else:
            await getattr(table, operation)()


@pytest.mark.asyncio
async def test_open_table_queries(mem_db_async: AsyncConnection):
    table = await mem_db_async.create_table("open_query", data=[{"id": 0}])
    assert await table.count_rows() == 1
    assert (await table.query().to_arrow()).to_pylist() == [{"id": 0}]
    assert (await table.head()).to_pylist() == [{"id": 0}]
    table.close()
