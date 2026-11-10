# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

import subprocess
import sys
import textwrap

import pytest


_WITHOUT_LANCE = """\
import importlib.abc
import sys

class MissingLance(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path, target=None):
        if fullname == "lance" or fullname.startswith("lance."):
            raise ModuleNotFoundError(fullname, name="lance")

sys.meta_path.insert(0, MissingLance())
"""


@pytest.mark.parametrize("api", ["sync", "async"])
@pytest.mark.parametrize("result", ["pandas", "batches", "lazy_error"])
def test_blob_query_without_pylance(tmp_path, api, result):
    if result == "pandas":
        pytest.importorskip("pandas")
    script = _WITHOUT_LANCE + textwrap.dedent(
        """\
        import asyncio
        import lancedb
        import pyarrow as pa

        schema = pa.schema([pa.field("id", pa.int64()), lancedb.blob("video")])
        rows = [{"id": 1, "video": b"one"}, {"id": 2, "video": None},
                {"id": 3, "video": b"three"}]

        def check(data):
            assert data == [{"filename": 2, "payload": None},
                            {"filename": 3, "payload": b"three"}], data

        async def main():
            if sys.argv[2] == "async":
                db = await lancedb.connect_async(sys.argv[1])
                table = await db.create_table("videos", schema=schema)
                await table.add(rows)
                query = table.query()
            else:
                db = lancedb.connect(sys.argv[1])
                table = db.create_table("videos", schema=schema)
                table.add(rows)
                query = table.search()
            query = query.select({"filename": "id", "payload": "video"}).offset(1)
            if sys.argv[3] == "pandas":
                frame = query.to_pandas(blob_mode="bytes", split_blocks=True)
                if sys.argv[2] == "async":
                    frame = await frame
                check(frame.to_dict("records"))
                frame = table.to_pandas(blob_mode="bytes")
                if sys.argv[2] == "async":
                    frame = await frame
                assert frame.to_dict("records") == rows
            elif sys.argv[3] == "batches":
                if sys.argv[2] == "async":
                    reader = await query.to_batches(
                        max_batch_length=1, blob_mode="bytes")
                    batches = [batch async for batch in reader]
                else:
                    reader = query.to_batches(batch_size=1, blob_mode="bytes")
                    batches = list(reader)
                assert reader.schema.field("payload").type == pa.large_binary()
                assert all(batch.num_rows <= 1 for batch in batches)
                assert all(batch.schema == reader.schema for batch in batches)
                check([row for batch in batches for row in batch.to_pylist()])
            else:
                try:
                    frame = query.to_pandas()
                    if sys.argv[2] == "async":
                        await frame
                except RuntimeError as error:
                    message = str(error)
                    assert "pip install pylance" in message, message
                    assert ".." not in message, message
                    assert "Use a plain scan query" not in message, message
                    assert "but The" not in message, message
                else:
                    raise AssertionError("lazy handles should require pylance")
            assert "lance" not in sys.modules

        asyncio.run(main())
        """
    )
    completed = subprocess.run(
        [sys.executable, "-c", script, str(tmp_path), api, result],
        capture_output=True,
        text=True,
    )
    assert completed.returncode == 0, completed.stderr


@pytest.mark.parametrize("api", ["sync", "async"])
@pytest.mark.parametrize("result", ["pandas", "batches", "arrow"])
def test_plain_legacy_blob_query_without_pylance(tmp_path, api, result):
    if result == "pandas":
        pytest.importorskip("pandas")
    script = _WITHOUT_LANCE + textwrap.dedent(
        """\
        import asyncio
        import lancedb
        import pyarrow as pa
        from lancedb.query import ColumnOrdering

        schema = pa.schema(
            [
                pa.field("id", pa.int64()),
                pa.field("filename", pa.string()),
                pa.field(
                    "video_blob",
                    pa.large_binary(),
                    metadata={"lance-encoding:blob": "true"},
                ),
            ]
        )
        rows = [
            {"id": 1, "filename": "file-1.mp4", "video_blob": b"outside"},
            {"id": 2, "filename": "file-2.mp4", "video_blob": b""},
            {"id": 3, "filename": "file-3.mp4", "video_blob": None},
            {"id": 4, "filename": "file-4.mp4", "video_blob": b"four"},
        ]
        expected = [
            {"filename": "file-2.mp4", "payload": b""},
            {"filename": "file-3.mp4", "payload": None},
            {"filename": "file-4.mp4", "payload": b"four"},
        ]

        def configure(query):
            return (
                query.select({"filename": "filename", "payload": "video_blob"})
                .where("id >= 2")
                .order_by(
                    [ColumnOrdering(column_name="id", ascending=True)]
                )
                .limit(3)
            )

        async def main():
            storage_options = {"new_table_data_storage_version": "2.1"}
            if sys.argv[2] == "async":
                db = await lancedb.connect_async(
                    sys.argv[1], storage_options=storage_options
                )
                table = await db.create_table("legacy", schema=schema)
                await table.add(rows)
                table = await db.open_table("legacy")
                def query():
                    return configure(table.query())
                def original_query():
                    return table.query().select(
                        {"filename": "filename", "payload": "video_blob"}
                    ).limit(1)
                def empty_query():
                    return query().where("id < 0")
                def batches():
                    return query().to_batches(
                        max_batch_length=1, blob_mode="bytes"
                    )
            else:
                db = lancedb.connect(
                    sys.argv[1], storage_options=storage_options
                )
                table = db.create_table("legacy", schema=schema)
                table.add(rows)
                table = db.open_table("legacy")
                def query():
                    return configure(table.search())
                def original_query():
                    return table.search().select(
                        {"filename": "filename", "payload": "video_blob"}
                    ).limit(1)
                def empty_query():
                    return query().where("id < 0")
                def batches():
                    return query().to_batches(
                        batch_size=1, blob_mode="bytes"
                    )

            if sys.argv[3] == "pandas":
                frame = query().to_pandas(blob_mode="bytes")
                if sys.argv[2] == "async":
                    frame = await frame
                assert frame.to_dict("records") == expected
                original = original_query().to_pandas(blob_mode="bytes")
                if sys.argv[2] == "async":
                    original = await original
                assert original.to_dict("records") == [
                    {"filename": "file-1.mp4", "payload": b"outside"}
                ]
                direct = table.to_pandas(blob_mode="bytes")
                if sys.argv[2] == "async":
                    direct = await direct
                assert direct.to_dict("records") == rows
                empty = empty_query().to_pandas(blob_mode="bytes")
                if sys.argv[2] == "async":
                    empty = await empty
                assert empty.to_dict("records") == []
            elif sys.argv[3] == "batches":
                reader = batches()
                if sys.argv[2] == "async":
                    reader = await reader
                    rows_out = [batch async for batch in reader]
                else:
                    rows_out = list(reader)
                assert reader.schema.field("payload").type == pa.large_binary()
                assert "_rowid" not in reader.schema.names
                assert all(batch.num_rows == 1 for batch in rows_out)
                assert all(batch.schema == reader.schema for batch in rows_out)
                actual = [row for batch in rows_out for row in batch.to_pylist()]
                assert actual == expected
                original_reader = original_query().to_batches(blob_mode="bytes")
                if sys.argv[2] == "async":
                    original_reader = await original_reader
                    original_rows = [batch async for batch in original_reader]
                else:
                    original_rows = list(original_reader)
                assert [
                    row for batch in original_rows for row in batch.to_pylist()
                ] == [{"filename": "file-1.mp4", "payload": b"outside"}]
                empty_reader = empty_query().to_batches(blob_mode="bytes")
                if sys.argv[2] == "async":
                    empty_reader = await empty_reader
                    empty_batches = [batch async for batch in empty_reader]
                else:
                    empty_batches = list(empty_reader)
                assert empty_batches == []
                assert empty_reader.schema.field("payload").type == pa.large_binary()
            else:
                table_out = query().to_arrow(blob_mode="bytes")
                if sys.argv[2] == "async":
                    table_out = await table_out
                assert table_out.schema.field("payload").type == pa.large_binary()
                assert "_rowid" not in table_out.column_names
                assert table_out.to_pylist() == expected
                original = original_query().to_arrow(blob_mode="bytes")
                if sys.argv[2] == "async":
                    original = await original
                assert original.to_pylist() == [
                    {"filename": "file-1.mp4", "payload": b"outside"}
                ]
                direct = table.to_arrow(blob_mode="bytes")
                if sys.argv[2] == "async":
                    direct = await direct
                assert direct.to_pylist() == rows
                empty_table = empty_query().to_arrow(blob_mode="bytes")
                if sys.argv[2] == "async":
                    empty_table = await empty_table
                assert empty_table.num_rows == 0
                assert empty_table.schema.field("payload").type == pa.large_binary()
            assert "lance" not in sys.modules

        asyncio.run(main())
        """
    )
    completed = subprocess.run(
        [sys.executable, "-c", script, str(tmp_path), api, result],
        capture_output=True,
        text=True,
    )
    assert completed.returncode == 0, completed.stderr
