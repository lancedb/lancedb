# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

import subprocess
import sys
import textwrap

import pytest


@pytest.mark.parametrize("with_pylance", [False, True])
def test_blob_array(with_pylance):
    # Use fresh processes so each extension class can own Arrow's registry.
    script = textwrap.dedent(
        """\
        import importlib.abc
        import sys

        if sys.argv[1] == "False":
            class MissingLance(importlib.abc.MetaPathFinder):
                def find_spec(self, fullname, path, target=None):
                    if fullname == "lance" or fullname.startswith("lance."):
                        raise ModuleNotFoundError(fullname, name="lance")
            sys.meta_path.insert(0, MissingLance())

        import lancedb
        import pyarrow as pa
        from lancedb.schema import blob, blob_array

        if sys.argv[1] == "True":
            from lance.blob import BlobType
        else:
            from lancedb.schema import _FallbackBlobType as BlobType

        schema = pa.schema([blob("video")])
        payloads = [b"abc", None, b"", b"\\x00\\xff"]
        for values in ([], [None, None], payloads):
            for source in (values, iter(values)):
                array = blob_array(source)
                assert isinstance(array, pa.ExtensionArray)
                assert type(array.type) is BlobType
                assert array.type == schema.field("video").type
                assert array.null_count == values.count(None)
                assert array.storage.to_pylist() == [
                    None if value is None else {
                        "data": value, "uri": None, "position": None, "size": None
                    }
                    for value in values
                ]
                batch = pa.RecordBatch.from_arrays([array], schema=schema)
                with pa.BufferOutputStream() as sink:
                    with pa.ipc.new_stream(sink, schema) as writer:
                        writer.write_batch(batch)
                    restored = pa.ipc.open_stream(sink.getvalue()).read_next_batch()
                assert restored.equals(batch)

        for invalid in ("file:///video", {"data": b"abc"}, 123):
            try:
                blob_array([invalid])
            except TypeError as error:
                assert "bytes or None" in str(error)
            else:
                raise AssertionError(f"accepted unsupported payload: {invalid!r}")

        # Batch iterators are the ingestion path that needs explicit arrays.
        batches = [
            pa.RecordBatch.from_arrays([blob_array([value])], schema=schema)
            for value in payloads
        ]
        table = lancedb.connect("memory:///").create_table(
            "videos", data=iter(batches[:2]), schema=schema
        )
        table.add(iter(batches[2:]))
        row_ids = table.search().with_row_id(True).to_arrow()["_rowid"]
        assert table.fetch_blobs("video", row_ids).to_pylist() == payloads
        if sys.argv[1] == "False":
            assert "lance" not in sys.modules
        """
    )
    result = subprocess.run(
        [sys.executable, "-c", script, str(with_pylance)],
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stderr
