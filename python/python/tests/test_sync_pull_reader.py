# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

import gc
import time
from datetime import timedelta

import pyarrow as pa
import pytest

import lancedb
import lancedb.table as table_module
from lancedb.query import AsyncQuery


def _table(db, name="sync_pull_reader"):
    return db.create_table(
        name,
        data=[{"id": i, "vector": [float(i), float(i)]} for i in range(10)],
    )


def _spy_on_pull_reader(monkeypatch):
    calls = []
    real = AsyncQuery._execute_batches_sync

    def spy(self, *, max_batch_length=None, timeout=None):
        calls.append((max_batch_length, timeout))
        return real(self, max_batch_length=max_batch_length, timeout=timeout)

    monkeypatch.setattr(AsyncQuery, "_execute_batches_sync", spy)
    return calls


@pytest.mark.parametrize(
    "batch_size,row_limit",
    [
        (None, 1024),
        (0, None),
        (4, 4),
    ],
)
def test_plain_sync_batches_use_pull_reader(mem_db, monkeypatch, batch_size, row_limit):
    table = _table(mem_db)
    calls = _spy_on_pull_reader(monkeypatch)

    reader = table.search().select(["id"]).to_batches(batch_size=batch_size)

    assert isinstance(reader, pa.RecordBatchReader)
    assert calls == [(batch_size, None)]
    batches = list(reader)
    assert sum(batch.num_rows for batch in batches) == 10
    assert pa.Table.from_batches(batches).column("id").to_pylist() == list(range(10))
    if row_limit is not None:
        assert all(batch.num_rows <= row_limit for batch in batches)


def test_plain_sync_batches_do_not_enter_background_loop(mem_db, monkeypatch):
    table = _table(mem_db)

    def fail(*args, **kwargs):
        raise AssertionError("plain sync batches entered the background event loop")

    monkeypatch.setattr(table_module.LOOP, "run", fail)

    reader = table.search().select(["id"]).to_batches(batch_size=4)

    assert sum(batch.num_rows for batch in reader) == 10


@pytest.mark.parametrize(
    "execute",
    [
        lambda table: table.search().select(["id"]).to_arrow(),
        lambda table: table.search([0.0, 0.0]).to_batches(),
        lambda table: table.take_offsets([0]).to_batches(),
        lambda table: table.search().select(["id"]).order_by(["id"]).to_batches(),
        lambda table: (
            table.search().select(["id"]).to_batches(blob_mode="descriptions")
        ),
    ],
)
def test_other_sync_routes_keep_the_existing_reader(mem_db, monkeypatch, execute):
    table = _table(mem_db)
    calls = _spy_on_pull_reader(monkeypatch)

    execute(table)

    assert calls == []


@pytest.mark.asyncio
async def test_async_batches_keep_the_existing_reader(monkeypatch):
    db = await lancedb.connect_async("memory://")
    table = await db.create_table(
        "async_sync_pull_reader", data=[{"id": i} for i in range(4)]
    )
    calls = _spy_on_pull_reader(monkeypatch)

    reader = await table.query().to_batches()

    total_rows = 0
    async for batch in reader:
        total_rows += batch.num_rows
    assert total_rows == 4
    assert calls == []


def test_timeout_starts_on_first_read_and_spans_reads(tmp_path):
    table = lancedb.connect(tmp_path).create_table(
        "timeout_pull_reader", data=[{"id": i} for i in range(8)]
    )
    reader = (
        table.search()
        .select(["id"])
        .to_batches(batch_size=2, timeout=timedelta(milliseconds=200))
    )

    # A slow consumer before the first poll must not count against the query:
    # the timeout starts when the first batch is requested.
    time.sleep(0.25)
    first = reader.read_next_batch()
    assert first.num_rows == 2

    # The deadline spans reads, so a pause between batches expires it.
    time.sleep(0.25)
    with pytest.raises(RuntimeError, match="Query timeout"):
        reader.read_next_batch()


def test_early_break_can_resume_the_same_reader(mem_db):
    table = _table(mem_db)
    reader = table.search().select(["id"]).to_batches(batch_size=2)

    first = next(iter(reader))
    rest = list(reader)

    assert pa.Table.from_batches([first, *rest]).column("id").to_pylist() == list(
        range(10)
    )

    reader = table.search().select(["id"]).to_batches(batch_size=2)
    for first in reader:
        break
    rest = list(reader)
    assert pa.Table.from_batches([first, *rest]).column("id").to_pylist() == list(
        range(10)
    )


def test_arrow_export_shares_cursor_with_the_last_owner(mem_db):
    table = _table(mem_db)
    reader = table.search().select(["id"]).to_batches(batch_size=2)
    first = reader.read_next_batch()

    capsule = reader.__arrow_c_stream__()
    del reader
    gc.collect()

    imported = pa.RecordBatchReader._import_from_c_capsule(capsule)
    rest = imported.read_all()
    combined = pa.concat_tables([pa.Table.from_batches([first]), rest])
    assert combined.column("id").to_pylist() == list(range(10))


def test_empty_result_preserves_the_query_schema(mem_db):
    table = _table(mem_db)
    query = table.search().select(["id"]).where("id < 0")

    reader = query.to_batches(batch_size=4)

    assert reader.schema == query.output_schema()
    assert reader.read_all().num_rows == 0
