# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

from datetime import timedelta

import pyarrow as pa
import pytest

import lancedb


@pytest.mark.asyncio
async def test_execute_to_reader_collects_without_default_slicing(tmp_path):
    db = await lancedb.connect_async(tmp_path)
    rows = 5000
    data = pa.table(
        {
            "id": pa.array(range(rows), type=pa.int64()),
            "text": pa.array([f"row-{i}" for i in range(rows)]),
        }
    )
    table = await db.create_table("test", data)

    reader = await table._inner.query().execute_to_reader(None, None)
    batches = list(reader)

    assert sum(batch.num_rows for batch in batches) == rows
    assert max(batch.num_rows for batch in batches) > 1024


@pytest.mark.asyncio
async def test_execute_to_reader_preserves_empty_schema(tmp_path):
    db = await lancedb.connect_async(tmp_path)
    data = pa.table(
        {
            "id": pa.array([1, 2], type=pa.int64()),
            "text": pa.array(["a", "b"]),
        }
    )
    table = await db.create_table("test", data)

    query = table._inner.query()
    query.where("id < 0")
    reader = await query.execute_to_reader(None, None)
    result = pa.Table.from_batches(reader, schema=reader.schema)

    assert result.num_rows == 0
    assert result.schema == await table.query().where("id < 0").output_schema()


@pytest.mark.asyncio
async def test_execute_to_reader_propagates_timeout(tmp_path):
    db = await lancedb.connect_async(tmp_path)
    table = await db.create_table("test", data=[{"id": 1}, {"id": 2}])

    with pytest.raises(Exception, match="Query timeout"):
        await table._inner.query().execute_to_reader(None, timedelta(0))
