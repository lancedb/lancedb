# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

import subprocess
import sys
import textwrap

import pytest


@pytest.mark.parametrize("api", ["sync", "async"])
@pytest.mark.parametrize("result", ["pandas", "batches", "lazy_error"])
def test_blob_query_without_pylance(tmp_path, api, result):
    script = """\
import importlib.abc
import sys

class MissingLance(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path, target=None):
        if fullname == "lance" or fullname.startswith("lance."):
            raise ModuleNotFoundError(fullname, name="lance")

sys.meta_path.insert(0, MissingLance())
""" + textwrap.dedent(
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
