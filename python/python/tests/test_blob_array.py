# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

import subprocess
import sys
import textwrap

import pytest


@pytest.mark.parametrize("with_pylance", [False, True])
def test_blob_array(with_pylance):
    if with_pylance:
        pytest.importorskip("lance")
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


@pytest.mark.parametrize("with_pylance", [False, True])
def test_blob_uri_array(with_pylance):
    if with_pylance:
        pytest.importorskip("lance")
    # Use fresh processes so each extension class can own Arrow's registry.
    script = textwrap.dedent(
        """\
        import importlib.abc
        import pathlib
        import sys
        import tempfile

        if sys.argv[1] == "False":
            class MissingLance(importlib.abc.MetaPathFinder):
                def find_spec(self, fullname, path, target=None):
                    if fullname == "lance" or fullname.startswith("lance."):
                        raise ModuleNotFoundError(fullname, name="lance")
            sys.meta_path.insert(0, MissingLance())

        import lancedb
        import pyarrow as pa
        from lancedb import BasePath, blob_uri_array
        from lancedb.schema import blob

        if sys.argv[1] == "True":
            from lance import DatasetBasePath
            from lance.blob import BlobType
        else:
            from lancedb.schema import _FallbackBlobType as BlobType
            DatasetBasePath = None

        schema = pa.schema([blob("video")])
        array = blob_uri_array(
            ["s3://bucket/a.mp4", None, "b.mp4"],
            positions=[0, None, None],
            sizes=[4, None, None],
        )
        assert isinstance(array, pa.ExtensionArray)
        assert type(array.type) is BlobType
        assert array.type == schema.field("video").type
        assert array.storage.to_pylist() == [
            {"data": None, "uri": "s3://bucket/a.mp4", "position": 0, "size": 4},
            None,
            {"data": None, "uri": "b.mp4", "position": None, "size": None},
        ]
        whole = blob_uri_array(iter(["s3://bucket/a.mp4"]))
        assert whole.storage.to_pylist() == [
            {"data": None, "uri": "s3://bucket/a.mp4", "position": None, "size": None}
        ]

        checks = [
            (lambda: blob_uri_array(["u"], positions=[0]), "provided together"),
            (
                lambda: blob_uri_array(["u"], positions=[0, 0], sizes=[1, 1]),
                "same length",
            ),
            (
                lambda: blob_uri_array(["u"], sizes=[1, 1], positions=[0, 0]),
                "same length",
            ),
            (
                lambda: blob_uri_array(["u"], positions=[0], sizes=[None]),
                "both position and size",
            ),
            (lambda: blob_uri_array(["u"], positions=[0], sizes=[0]), "positive"),
            (lambda: blob_uri_array(["u"], positions=[-1], sizes=[1]), "non-negative"),
            (lambda: blob_uri_array([None], positions=[0], sizes=[1]), "None but"),
            (lambda: blob_uri_array(["u"], positions=[0], sizes=[2**64]), "uint64"),
            (lambda: blob_uri_array([b"u"]), "must be a string"),
        ]
        for call, fragment in checks:
            try:
                call()
            except (TypeError, ValueError, OverflowError) as error:
                assert fragment in str(error), (fragment, error)
            else:
                raise AssertionError(f"accepted invalid input: {fragment}")

        try:
            BasePath("s3://bucket", id=0)
        except ValueError as error:
            assert "id=None" in str(error)
        else:
            raise AssertionError("accepted explicit id=0")

        # The public create route must accept the helper and register the base
        # in the same commit as the data, with or without pylance.
        root = pathlib.Path(tempfile.mkdtemp())
        base_dir = root / "videos"
        base_dir.mkdir()
        payload = b"0123456789abcdef"
        clip = base_dir / "clip.mp4"
        clip.write_bytes(payload)
        db = lancedb.connect(root / "db")
        field = blob("video").with_metadata({"lancedb:logical_type": "mp4"})
        table_schema = pa.schema([pa.field("id", pa.int64()), field])
        videos = blob_uri_array([clip.as_uri()], positions=[0], sizes=[6])
        batch = pa.RecordBatch.from_arrays(
            [pa.array([1], pa.int64()), videos], schema=table_schema
        )
        if DatasetBasePath is not None:
            bases = [DatasetBasePath(path=base_dir.as_uri(), name="source")]
        else:
            bases = [BasePath(path=base_dir.as_uri(), name="source")]
        table = db.create_table("videos", data=batch, initial_bases=bases)
        assert table.version == 1
        assert (
            table.schema.field("video").metadata[b"lancedb:logical_type"] == b"mp4"
        )
        row_ids = table.search().with_row_id(True).to_arrow()["_rowid"]
        assert table.fetch_blobs("video", row_ids).to_pylist() == [payload[:6]]
        assert table.fetch_blob_ranges(
            "video", [(row_ids[0].as_py(), 1, 3)]
        ).to_pylist() == [payload[1:4]]
        if DatasetBasePath is not None:
            base_paths = table.to_lance().base_paths()
            assert base_paths[1].path == base_dir.as_uri()
            assert base_paths[1].is_dataset_root is False
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
