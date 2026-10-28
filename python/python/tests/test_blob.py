# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

import ctypes
import io
import subprocess
import sys
import textwrap

import lance
import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc
import pytest
from lance.blob import Blob, BlobType as LanceBlobType

import lancedb
from lancedb import schema as schema_module
from lancedb._blob import (
    _set_blob_column,
    blob_v2_projection_sources,
    read_row_ids_from_hits,
    stash_auto_row_ids,
)
from lancedb.expr import col
from lancedb.index import FTS
from lancedb.rerankers import Reranker
from lancedb.schema import blob_column_paths, blob_v2_column_paths


_HIDE_LANCE_BLOB = """\
import importlib.abc
import sys

class _MissingLanceBlob(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path, target=None):
        if fullname == "lance.blob" or fullname.startswith("lance.blob."):
            raise ModuleNotFoundError(fullname, name="lance.blob")

sys.modules.pop("lance.blob", None)
sys.meta_path.insert(0, _MissingLanceBlob())
"""


def _blob_table(name, rows):
    db = lancedb.connect("memory:///")
    schema = pa.schema([pa.field("id", pa.int64()), lancedb.blob("image")])
    table = db.create_table(name, schema=schema)
    table.add(rows)
    return table


def _blob_array(name, values):
    blob_type = lancedb.blob(name).type
    storage_type = blob_type.storage_type
    storage = pa.StructArray.from_arrays(
        [
            pa.array(values, type=pa.large_binary()),
            pa.array([None] * len(values), type=pa.string()),
            pa.array([None] * len(values), type=pa.uint64()),
            pa.array([None] * len(values), type=pa.uint64()),
        ],
        fields=list(storage_type),
    )
    return pa.ExtensionArray.from_storage(blob_type, storage)


def _blob_layout_data(rows, with_ranges, *, as_extension=True):
    field = lancedb.blob("image")
    storage_type = field.type.storage_type
    if not with_ranges or not as_extension:
        storage_type = pa.struct(list(storage_type)[: (4 if with_ranges else 2)])
        field = pa.field(
            "image",
            storage_type,
            metadata={"ARROW:extension:name": "lance.blob.v2"},
        )
    images = pa.array([row["image"] for row in rows], type=storage_type)
    if isinstance(field.type, pa.ExtensionType):
        images = pa.ExtensionArray.from_storage(field.type, images)
    return pa.Table.from_arrays(
        [pa.array([row["id"] for row in rows], type=pa.int64()), images],
        schema=pa.schema([pa.field("id", pa.int64()), field]),
    )


def _row_ids_by_id(table):
    hits = table.search().with_row_id(True).limit(1000).to_arrow()
    assert "_rowid" in hits.column_names
    return dict(zip(hits["id"].to_pylist(), hits["_rowid"].to_pylist()))


def _assert_missing_blob_row_ids(exc_info):
    message = str(exc_info.value)
    assert "row ids but some do not exist in the table" in message
    assert "rowaddr" not in message
    assert "fragment" not in message


def _assert_fetch_apis_reject_missing_row_ids(table, row_ids):
    expected = f"first missing row ids: [{row_ids[0]}]"
    with pytest.raises(ValueError) as exc_info:
        table.fetch_blobs("image", row_ids)
    _assert_missing_blob_row_ids(exc_info)
    assert expected in str(exc_info.value)
    with pytest.raises(ValueError) as exc_info:
        table.fetch_blob_files("image", row_ids)
    _assert_missing_blob_row_ids(exc_info)
    assert expected in str(exc_info.value)
    with pytest.raises(ValueError) as exc_info:
        table.fetch_blob_ranges("image", [(row_id, 0, 1) for row_id in row_ids])
    _assert_missing_blob_row_ids(exc_info)
    assert expected in str(exc_info.value)


def test_blob_factory_declares_v2_field():
    field = lancedb.blob("image")
    assert isinstance(field.type, pa.ExtensionType)
    assert field.type.extension_name == "lance.blob.v2"
    assert lancedb.BlobType is LanceBlobType
    assert type(field.type) is LanceBlobType


def test_blob_thresholds_land_in_field_metadata():
    field = lancedb.blob(
        "payload",
        inline_size_threshold=8 * 1024,
        dedicated_size_threshold=1 << 20,
        pack_file_size_threshold=1 << 26,
    )
    assert field.type.extension_name == "lance.blob.v2"
    metadata = field.metadata
    assert metadata[b"lance-encoding:blob-inline-size-threshold"] == b"8192"
    assert metadata[b"lance-encoding:blob-dedicated-size-threshold"] == b"1048576"
    assert metadata[b"lance-encoding:blob-pack-file-size-threshold"] == b"67108864"


def test_blob_without_thresholds_has_no_threshold_metadata():
    field = lancedb.blob("image")
    metadata = field.metadata or {}
    assert not any(b"threshold" in key for key in metadata)


@pytest.mark.parametrize("nullable", [False, True])
@pytest.mark.parametrize(
    "thresholds",
    [
        {},
        {"inline_size_threshold": 0},
        {"inline_size_threshold": 8192},
        {"dedicated_size_threshold": 1 << 20},
        {"pack_file_size_threshold": 1 << 26},
        {
            "inline_size_threshold": 8192,
            "dedicated_size_threshold": 1 << 20,
            "pack_file_size_threshold": 1 << 26,
        },
        {
            "inline_size_threshold": ctypes.c_size_t(-1).value,
            "dedicated_size_threshold": ctypes.c_size_t(-1).value,
            "pack_file_size_threshold": ctypes.c_size_t(-1).value,
        },
    ],
)
def test_blob_thresholds_match_pylance(monkeypatch, nullable, thresholds):
    expected = lance.blob_field("payload", nullable=nullable, **thresholds)
    actual = lancedb.blob("payload", nullable=nullable, **thresholds)
    assert type(actual.type) is LanceBlobType
    assert actual.equals(expected, check_metadata=True)

    # Both extension classes must encode the same Arrow field on the wire.
    monkeypatch.setattr(
        schema_module, "_resolve_blob_type", lambda: schema_module._FallbackBlobType
    )
    fallback = lancedb.blob("payload", nullable=nullable, **thresholds)
    assert pa.schema([fallback]).serialize() == pa.schema([expected]).serialize()


def test_blob_thresholds_are_validated():
    with pytest.raises(ValueError, match="inline_size_threshold must be non-negative"):
        lancedb.blob("payload", inline_size_threshold=-1)
    with pytest.raises(ValueError, match="dedicated_size_threshold must be positive"):
        lancedb.blob("payload", dedicated_size_threshold=0)
    with pytest.raises(ValueError, match="pack_file_size_threshold must be positive"):
        lancedb.blob("payload", pack_file_size_threshold=0)
    with pytest.raises(TypeError, match="inline_size_threshold must be an int"):
        lancedb.blob("payload", inline_size_threshold="8192")


def test_blob_type_works_without_pylance():
    script = _HIDE_LANCE_BLOB + textwrap.dedent(
        """\
        import lancedb
        import pyarrow as pa

        field = lancedb.blob("image")
        if not isinstance(field.type, pa.ExtensionType):
            raise SystemExit("expected an extension type")
        if field.type.extension_name != "lance.blob.v2":
            raise SystemExit(field.type.extension_name)
        if lancedb.BlobType is not type(field.type):
            raise SystemExit("BlobType is not the field type class")
        if lancedb.BlobType.__module__ != "lancedb.schema":
            raise SystemExit(lancedb.BlobType.__module__)

        db = lancedb.connect("memory:///")
        table = db.create_table(
            "images",
            schema=pa.schema([pa.field("id", pa.int64()), field]),
        )
        table.add([{"id": 1, "image": b"hello"}])
        result = (
            table.merge_insert("id")
            .when_matched_update_all()
            .when_not_matched_insert_all()
            .execute([{"id": 1, "image": b"updated"}, {"id": 2, "image": b"inserted"}])
        )
        if result.num_updated_rows != 1 or result.num_inserted_rows != 1:
            raise SystemExit(
                f"merge_insert rows updated={result.num_updated_rows} "
                f"inserted={result.num_inserted_rows}"
            )
        """
    )
    result = subprocess.run(
        [sys.executable, "-c", script],
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, result.stderr


def test_blob_resolves_pylance_type_without_eager_import():
    script = textwrap.dedent(
        """\
        import sys
        import lancedb

        if "lance.blob" in sys.modules:
            raise SystemExit("import lancedb imported lance.blob")
        field = lancedb.blob("image")
        from lance.blob import BlobType

        if type(field.type) is not BlobType:
            raise SystemExit(f"{type(field.type)} is not {BlobType}")
        import lance

        image = lance.blob_array([b"x"])
        if type(image.type) is not BlobType:
            raise SystemExit("blob_array used a different class")
        if type(image.type) is not type(field.type):
            raise SystemExit("field and array classes differ")
        """
    )
    result = subprocess.run(
        [sys.executable, "-c", script],
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, result.stderr


def test_blob_fallback_fails_if_name_already_registered():
    script = _HIDE_LANCE_BLOB + textwrap.dedent(
        """\
        import pyarrow as pa

        class OtherBlobType(pa.ExtensionType):
            def __init__(self):
                super().__init__(
                    pa.struct([pa.field("data", pa.large_binary())]),
                    "lance.blob.v2",
                )

            def __arrow_ext_serialize__(self):
                return b""

            @classmethod
            def __arrow_ext_deserialize__(cls, storage_type, serialized):
                return cls()

        pa.register_extension_type(OtherBlobType())
        import lancedb

        try:
            lancedb.blob("image")
        except ValueError as err:
            if "already registered" not in str(err):
                raise SystemExit(err)
        else:
            raise SystemExit("expected ValueError")
        """
    )
    result = subprocess.run(
        [sys.executable, "-c", script],
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, result.stderr


def test_blob_type_rejects_competing_registration_with_pylance():
    script = textwrap.dedent(
        """\
        import pyarrow as pa
        import pyarrow.ipc

        class OtherBlobType(pa.ExtensionType):
            def __init__(self):
                super().__init__(
                    pa.struct(
                        [
                            pa.field("data", pa.large_binary()),
                            pa.field("uri", pa.utf8()),
                            pa.field("position", pa.uint64()),
                            pa.field("size", pa.uint64()),
                        ]
                    ),
                    "lance.blob.v2",
                )

            def __arrow_ext_serialize__(self):
                return b""

            @classmethod
            def __arrow_ext_deserialize__(cls, storage_type, serialized):
                return cls()

        pa.register_extension_type(OtherBlobType())

        from lance.blob import BlobType

        if BlobType is OtherBlobType:
            raise SystemExit("pylance BlobType was replaced")
        schema = pa.schema([pa.field("value", BlobType())])
        restored = pa.ipc.read_schema(schema.serialize())
        if type(restored.field("value").type) is not OtherBlobType:
            raise SystemExit(type(restored.field("value").type))

        import lancedb

        try:
            lancedb.blob("image")
        except ValueError as err:
            if "__main__.OtherBlobType" not in str(err):
                raise SystemExit(err)
        else:
            raise SystemExit("expected ValueError")
        """
    )
    result = subprocess.run(
        [sys.executable, "-c", script],
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, result.stderr


def test_blob_v2_column_paths_include_list_children():
    schema = pa.schema(
        [
            pa.field("id", pa.int64()),
            pa.field("info", pa.struct([lancedb.blob("blob")])),
            pa.field("images", pa.list_(lancedb.blob("image"))),
            pa.field("large_images", pa.large_list(lancedb.blob("large_image"))),
            pa.field(
                "fixed_images",
                pa.list_(lancedb.blob("fixed_image"), list_size=2),
            ),
        ]
    )

    assert blob_v2_column_paths(schema) == [
        "info.blob",
        "images.image",
        "large_images.large_image",
        "fixed_images.fixed_image",
    ]


def test_blob_v2_projection_sources_use_typed_column_name():
    schema = pa.schema([lancedb.blob("blob")])

    assert blob_v2_projection_sources(schema, {"blob_alias": col("blob")}) == {
        "blob_alias": "blob"
    }


def test_blob_v2_projection_sources_include_selected_struct():
    schema = pa.schema(
        [
            pa.field(
                "info", pa.struct([pa.field("name", pa.string()), lancedb.blob("blob")])
            )
        ]
    )

    assert blob_v2_projection_sources(schema, ["info"]) == {"info.blob": "info.blob"}
    assert blob_v2_projection_sources(schema, {"renamed": "info"}) == {
        "renamed.blob": "info.blob"
    }


@pytest.mark.parametrize("parent_path", ["info", "outer.info"])
def test_set_blob_column_missing_nested_field_raises(parent_path):
    row = {"name": "first"}
    for name in reversed(parent_path.split(".")):
        row = {name: row}
    table = pa.Table.from_pylist([row])
    blobs = pa.array([b"hello"], type=pa.large_binary())

    with pytest.raises(KeyError) as exc_info:
        _set_blob_column(table, f"{parent_path}.blob", blobs)

    assert parent_path in str(exc_info.value)
    assert "'blob'" in str(exc_info.value)


def _nested_blob_table(db, name):
    schema = pa.schema(
        [
            pa.field("id", pa.int64()),
            pa.field(
                "info", pa.struct([pa.field("name", pa.string()), lancedb.blob("blob")])
            ),
        ]
    )
    table = db.create_table(name, schema=schema)
    info = pa.StructArray.from_arrays(
        [
            pa.array(["first", "second", "third"]),
            pa.array([b"hello", None, b"hidden"], type=pa.large_binary()),
        ],
        names=["name", "blob"],
        mask=pa.array([False, False, True]),
    )
    table.add(pa.table({"id": pa.array([0, 1, 2]), "info": info}))
    return table


def test_nested_blob_to_pandas_bytes(tmp_path):
    table = _nested_blob_table(lancedb.connect(tmp_path), "nested_pandas_bytes")
    expected = [
        {"name": "first", "blob": b"hello"},
        {"name": "second", "blob": None},
        None,
    ]

    assert table.to_pandas(blob_mode="bytes")["info"].tolist() == expected
    assert table.search().to_pandas(blob_mode="bytes")["info"].tolist() == expected
    assert (
        table.search().select(["info"]).to_pandas(blob_mode="bytes")["info"].tolist()
        == expected
    )
    assert (
        table.search()
        .select({"renamed": "info"})
        .to_pandas(blob_mode="bytes")["renamed"]
        .tolist()
        == expected
    )
    assert table.search().select({"b": "info.blob"}).to_pandas(blob_mode="bytes")[
        "b"
    ].tolist() == [b"hello", None, None]


def test_to_arrow_and_to_list_blob_mode(tmp_path):
    table = _nested_blob_table(lancedb.connect(tmp_path), "arrow_blob_mode")
    expected = [
        {"name": "first", "blob": b"hello"},
        {"name": "second", "blob": None},
        None,
    ]

    assert table.to_arrow(blob_mode="bytes")["info"].to_pylist() == expected
    assert table.search().to_arrow(blob_mode="bytes")["info"].to_pylist() == expected
    assert table.search().to_list(blob_mode="bytes") == [
        {"id": 0, "info": expected[0]},
        {"id": 1, "info": expected[1]},
        {"id": 2, "info": expected[2]},
    ]
    assert table.search().select({"b": "info.blob"}).to_list(blob_mode="bytes") == [
        {"b": b"hello"},
        {"b": None},
        {"b": None},
    ]
    assert table.search().select(["id", "info.blob"]).to_list(blob_mode="bytes") == [
        {"id": 0, "info.blob": b"hello"},
        {"id": 1, "info.blob": None},
        {"id": 2, "info.blob": None},
    ]
    assert table.search().select({"x.y": "info"}).to_list(blob_mode="bytes") == [
        {"x.y": expected[0]},
        {"x.y": expected[1]},
        {"x.y": expected[2]},
    ]
    assert [
        row["info"] for row in table.search().select([]).to_list(blob_mode="bytes")
    ] == expected
    assert table.search().select([]).to_pandas(blob_mode="bytes")["info"].tolist() == (
        table.search().select([]).to_arrow(blob_mode="bytes")["info"].to_pylist()
    )
    assert "_rowid" not in table.search().select([]).to_arrow().column_names
    # A flat non-blob column named like the nested blob path keeps its values.
    shadowed = table.search().select({"info": "info", "info.blob": "id"})
    assert [row["info.blob"] for row in shadowed.to_list()] == [0, 1, 2]
    assert [row["info"] for row in shadowed.to_list(blob_mode="bytes")] == expected
    with pytest.raises(ValueError, match="both produce blob column 'info.blob'"):
        table.search().select(["info", "info.blob"]).to_list(blob_mode="bytes")

    default_blob = table.search().select({"b": "info.blob"}).to_list()[0]["b"]
    assert isinstance(default_blob, dict)
    lazy = table.to_arrow(blob_mode="lazy")["info"].to_pylist()
    assert table.to_arrow()["info"].to_pylist() == lazy
    lazy_descriptor = lazy[0]["blob"]
    assert "_lance_row_id" in lazy_descriptor
    lazy_descriptor.pop("_lance_row_id")
    descriptions = table.to_arrow(blob_mode="descriptions")["info"].to_pylist()
    assert descriptions[0]["blob"] == lazy_descriptor
    assert (
        table.search().to_list(blob_mode="descriptions")[0]["info"]["blob"]
        == lazy_descriptor
    )

    with pytest.raises(ValueError, match="blob_mode must be one of"):
        table.to_arrow(blob_mode="handles")
    with pytest.raises(ValueError, match="blob_mode must be one of"):
        table.search().to_list(blob_mode="handles")


@pytest.mark.asyncio
async def test_async_to_arrow_and_to_list_blob_mode(tmp_path):
    db = await lancedb.connect_async(tmp_path)
    table = await db.create_table(
        "async_arrow_blob_mode",
        schema=pa.schema(
            [
                pa.field("id", pa.int64()),
                pa.field(
                    "info",
                    pa.struct([pa.field("name", pa.string()), lancedb.blob("blob")]),
                ),
            ]
        ),
    )
    await table.add(
        pa.table(
            {
                "id": pa.array([0, 1]),
                "info": pa.StructArray.from_arrays(
                    [
                        pa.array(["first", "second"]),
                        pa.array([b"hello", None], type=pa.large_binary()),
                    ],
                    names=["name", "blob"],
                ),
            }
        )
    )
    expected = [
        {"name": "first", "blob": b"hello"},
        {"name": "second", "blob": None},
    ]

    assert (await table.to_arrow(blob_mode="bytes"))["info"].to_pylist() == expected
    assert (await table.query().to_arrow(blob_mode="bytes"))[
        "info"
    ].to_pylist() == expected
    assert await table.query().to_list(blob_mode="bytes") == [
        {"id": 0, "info": expected[0]},
        {"id": 1, "info": expected[1]},
    ]
    default_blob = (await table.query().select({"b": "info.blob"}).to_list())[0]["b"]
    assert isinstance(default_blob, dict)
    descriptions = await table.to_arrow(blob_mode="descriptions")
    assert (
        "_lance_row_id"
        not in descriptions.schema.field("info").type.field("blob").type.names
    )


def test_vector_query_to_arrow_blob_mode_bytes(tmp_path):
    db = lancedb.connect(tmp_path)
    schema = pa.schema(
        [
            pa.field("id", pa.int64()),
            pa.field("vector", pa.list_(pa.float32(), 2)),
            lancedb.blob("blob"),
        ]
    )
    table = db.create_table("vector_arrow_blob", schema=schema)
    table.add(
        [
            {"id": 1, "vector": [1.0, 0.0], "blob": b"one"},
            {"id": 2, "vector": [0.0, 1.0], "blob": b"two"},
        ]
    )
    table.add([{"id": 3, "vector": [0.9, 0.1], "blob": b"three"}])
    table.add([{"id": 4, "vector": [0.1, 0.9], "blob": None}])

    hits = table.search([0.0, 1.0]).limit(4).to_arrow(blob_mode="bytes").to_pylist()
    assert [(hit["id"], hit["blob"]) for hit in hits] == [
        (2, b"two"),
        (4, None),
        (3, b"three"),
        (1, b"one"),
    ]
    arrow = table.search([1.0, 0.0]).limit(1).to_arrow(blob_mode="bytes")
    assert arrow["blob"].to_pylist() == [b"one"]
    assert table.search([1.0, 0.0]).limit(1).to_list(blob_mode="bytes")[0]["blob"] == (
        b"one"
    )
    assert isinstance(
        table.search([1.0, 0.0]).limit(1).to_list()[0]["blob"],
        dict,
    )


@pytest.mark.asyncio
async def test_nested_blob_to_pandas_bytes_async(tmp_path):
    db = await lancedb.connect_async(tmp_path)
    schema = pa.schema(
        [
            pa.field("id", pa.int64()),
            pa.field(
                "info", pa.struct([pa.field("name", pa.string()), lancedb.blob("blob")])
            ),
        ]
    )
    table = await db.create_table("nested_pandas_bytes_async", schema=schema)
    await table.add(
        pa.table(
            {
                "id": pa.array([0, 1]),
                "info": pa.StructArray.from_arrays(
                    [
                        pa.array(["first", "second"]),
                        pa.array([b"hello", None], type=pa.large_binary()),
                    ],
                    names=["name", "blob"],
                ),
            }
        )
    )
    expected = [
        {"name": "first", "blob": b"hello"},
        {"name": "second", "blob": None},
    ]

    assert (await table.to_pandas(blob_mode="bytes"))["info"].tolist() == expected
    assert (await table.query().to_pandas(blob_mode="bytes"))[
        "info"
    ].tolist() == expected


def _legacy_v1_table(name):
    # Legacy v1 blob columns are only writable at file version <= 2.1.
    db = lancedb.connect(
        "memory:///", storage_options={"new_table_data_storage_version": "2.1"}
    )
    schema = pa.schema(
        [
            pa.field("id", pa.int64()),
            pa.field(
                "legacy", pa.large_binary(), metadata={"lance-encoding:blob": "true"}
            ),
        ]
    )
    table = db.create_table(name, schema=schema)
    table.add([{"id": 1, "legacy": b"old"}])
    return table


def test_blob_v2_column_paths_exclude_legacy_metadata():
    schema = pa.schema(
        [
            pa.field("id", pa.int64()),
            lancedb.blob("image"),
            pa.field(
                "legacy", pa.large_binary(), metadata={"lance-encoding:blob": "true"}
            ),
        ]
    )
    assert blob_v2_column_paths(schema) == ["image"]
    assert blob_column_paths(schema) == ["image", "legacy"]


def test_blob_v2_paths_match_blob_columns():
    table = _blob_table("paths_match", [{"id": 1, "image": b"x"}])
    assert blob_v2_column_paths(table.schema) == table.blob_columns()

    db = lancedb.connect("memory:///")
    info = pa.StructArray.from_arrays(
        [
            pa.array(["first"], type=pa.string()),
            _blob_array("blob", [b"nested"]),
        ],
        names=["name", "blob"],
    )
    data = pa.Table.from_arrays(
        [pa.array([1], type=pa.int64()), info],
        names=["id", "info"],
    )
    nested = db.create_table("nested_paths", data=data)
    assert blob_v2_column_paths(nested.schema) == nested.blob_columns()


def test_auto_row_id_stash_round_trip():
    table = _blob_table(
        "stash_round_trip",
        [{"id": 1, "image": b"alpha"}, {"id": 2, "image": b"beta"}],
    )
    hits = table.search().with_row_id(True).limit(10).to_arrow()
    row_ids = hits["_rowid"].to_pylist()

    stashed = stash_auto_row_ids(hits, ["image"])

    assert "_rowid" not in stashed.column_names
    assert stashed.schema.field("image").metadata == hits.schema.field("image").metadata
    assert read_row_ids_from_hits(stashed, "image") == row_ids


def test_blob_query_omits_auto_row_id():
    table = _blob_table("rowid", [{"id": 1, "image": b"x"}])
    hits = table.search().limit(10).to_arrow()
    assert "_rowid" not in hits.column_names


def test_blob_query_explicit_row_id_opt_in():
    table = _blob_table("explicit_rowid", [{"id": 1, "image": b"x"}])
    hits = table.search().with_row_id(True).limit(10).to_arrow()
    assert "_rowid" in hits.column_names


def test_table_to_pandas_descriptions_mode_omits_row_id():
    table = _blob_table("descriptions_no_leak", [{"id": 1, "image": b"x"}])
    df = table.to_pandas(blob_mode="descriptions")
    descriptor = df["image"].iloc[0]
    assert "_lance_row_id" not in descriptor
    assert set(descriptor.keys()) == {"kind", "position", "size", "blob_id", "blob_uri"}


@pytest.mark.asyncio
async def test_async_table_to_pandas_descriptions_mode_omits_row_id():
    db = await lancedb.connect_async("memory:///")
    schema = pa.schema([pa.field("id", pa.int64()), lancedb.blob("image")])
    table = await db.create_table("descriptions_no_leak_async", schema=schema)
    await table.add([{"id": 1, "image": b"x"}])
    df = await table.to_pandas(blob_mode="descriptions")
    descriptor = df["image"].iloc[0]
    assert "_lance_row_id" not in descriptor
    assert set(descriptor.keys()) == {"kind", "position", "size", "blob_id", "blob_uri"}


@pytest.mark.asyncio
async def test_async_calculated_select_does_not_read_every_blob_column():
    db = await lancedb.connect_async("memory:///calculated_select")
    schema = pa.schema([pa.field("id", pa.int64()), lancedb.blob("image")])
    table = await db.create_table("calculated_select", schema=schema)
    await table.add([{"id": 1, "image": b"hello"}])

    hits = await table.query().select({"next_id": col("id") + 1}).to_arrow()

    assert hits.column_names == ["next_id"]
    assert hits["next_id"].to_pylist() == [2]


@pytest.mark.asyncio
async def test_async_typed_blob_projection_preserves_source_column():
    db = await lancedb.connect_async("memory:///typed_blob_projection")
    schema = pa.schema([pa.field("id", pa.int64()), lancedb.blob("blob")])
    table = await db.create_table("typed_blob_projection", schema=schema)
    await table.add([{"id": 1, "blob": b"alpha"}])

    hits = await table.query().select({"blob_alias": col("blob")}).to_arrow()

    assert "_lance_row_id" in hits.schema.field("blob_alias").type.names
    blobs = await table.fetch_blobs("blob", hits)
    assert blobs.to_pylist() == [b"alpha"]


@pytest.mark.asyncio
@pytest.mark.parametrize("shape", ["scan", "hybrid"])
async def test_async_bytes_mode_rejects_blob_spelling_it_cannot_match(shape):
    db = await lancedb.connect_async("memory:///")
    table = await _hybrid_blob_table_async(db)
    query = table.query()
    if shape == "hybrid":
        query = query.nearest_to([1.0, 0.0]).nearest_to_text("hello")

    with pytest.raises(ValueError, match=r"could not match result columns \['pic'\]"):
        await query.select({"id": "id", "pic": "`image`"}).to_arrow(blob_mode="bytes")


def test_bytes_mode_keeps_struct_with_blob_descriptor_field_names():
    descriptor_like = pa.struct(
        [
            pa.field("kind", pa.uint8()),
            pa.field("position", pa.uint64()),
            pa.field("size", pa.uint64()),
            pa.field("blob_id", pa.uint32()),
            pa.field("blob_uri", pa.utf8()),
        ]
    )
    meta = {"kind": 1, "position": 2, "size": 3, "blob_id": 4, "blob_uri": "u"}
    table = lancedb.connect("memory:///").create_table(
        "descriptor_like",
        schema=pa.schema(
            [
                pa.field("id", pa.int64()),
                lancedb.blob("image"),
                pa.field("meta", descriptor_like),
            ]
        ),
    )
    table.add([{"id": 1, "image": b"one", "meta": meta}])

    assert table.search().to_list(blob_mode="bytes") == [
        {"id": 1, "image": b"one", "meta": meta}
    ]


@pytest.mark.parametrize("source", ["`image`", "IMAGE"])
def test_bytes_mode_rejects_blob_spelling_it_cannot_match(source):
    table = _blob_table("blob_spelling", [{"id": 1, "image": b"one"}])

    with pytest.raises(ValueError, match="could not match result columns"):
        table.search().select({"id": "id", "pic": source}).to_arrow(blob_mode="bytes")


def test_to_pandas_does_not_drop_fragment_selection_on_fallback():
    table = _blob_table("blob_fragments", [{"id": 1, "image": b"one"}])
    table.add([{"id": 2, "image": b"two"}])

    query = table.search().fragment_ids([0])
    with pytest.raises(RuntimeError, match="native pandas conversion"):
        query.to_pandas(blob_mode="bytes")
    with pytest.raises(RuntimeError, match="native pandas conversion"):
        table.search().with_row_address(True).to_pandas(blob_mode="descriptions")


def test_to_pandas_reports_lance_dataset_failures(tmp_path):
    table = lancedb.connect(tmp_path).create_table(
        "dataset_failure",
        schema=pa.schema([pa.field("id", pa.int64()), lancedb.blob("image")]),
    )
    table.add([{"id": 1, "image": b"one"}])

    def reject_credentials():
        raise PermissionError("storage credentials rejected")

    table.to_lance = reject_credentials
    with pytest.raises(PermissionError, match="storage credentials rejected"):
        table.search().to_pandas(blob_mode="lazy")


@pytest.mark.parametrize("blob_mode", ["lazy", "descriptions", "bytes"])
@pytest.mark.parametrize("with_row_id", [None, True, False])
def test_to_batches_blob_modes(tmp_path, monkeypatch, blob_mode, with_row_id):
    table = _nested_blob_table(lancedb.connect(tmp_path), "batch_blob_modes")
    query = table.search().select({"renamed": "info"})
    if with_row_id is not None:
        query.with_row_id(with_row_id)
    fetched = []
    fetch_blobs = table.fetch_blobs

    def fetch(column, row_ids):
        fetched.append(row_ids)
        return fetch_blobs(column, row_ids)

    monkeypatch.setattr(table, "fetch_blobs", fetch)
    reader = query.to_batches(batch_size=1, blob_mode=blob_mode)
    assert fetched == []
    first = reader.read_next_batch()
    assert first.schema == reader.schema
    assert len(fetched) == (1 if blob_mode == "bytes" else 0)
    batches = [first, *reader]
    assert all(batch.schema == reader.schema for batch in batches)
    assert all(batch.num_rows == 1 for batch in batches)
    assert len(fetched) == (3 if blob_mode == "bytes" else 0)
    if blob_mode == "bytes":
        assert [row["renamed"] for batch in batches for row in batch.to_pylist()] == [
            {"name": "first", "blob": b"hello"},
            {"name": "second", "blob": None},
            None,
        ]
    if blob_mode != "lazy":
        assert ("_rowid" in reader.schema.names) == (with_row_id is True)
        assert "_lance_row_id" not in str(reader.schema)
    else:
        assert "_rowid" in reader.schema.names


@pytest.mark.parametrize("blob_mode", ["descriptions", "bytes"])
def test_empty_to_batches_blob_schema(blob_mode):
    table = _blob_table("empty_blob_batches", [{"id": 1, "image": b"one"}])
    query = table.search().where("id < 0")
    reader = query.to_batches(blob_mode=blob_mode)
    assert reader.schema == query.to_arrow(blob_mode=blob_mode).schema
    assert reader.read_all().num_rows == 0


def test_to_batches_validates_blob_mode():
    table = _blob_table("invalid_batch_blob_mode", [{"id": 1, "image": b"one"}])
    with pytest.raises(ValueError, match="blob_mode must be one of"):
        table.search().to_batches(blob_mode="handles")


def test_to_pandas_keeps_to_arrow_overrides_without_blob_mode():
    from lancedb.query import LanceEmptyQueryBuilder

    class ToArrowWithoutBlobMode(LanceEmptyQueryBuilder):
        def to_arrow(self, *, timeout=None):
            return super().to_arrow(timeout=timeout)

    table = lancedb.connect("memory:///").create_table("no_blob", data=[{"id": 1}])
    query = ToArrowWithoutBlobMode(table)
    assert query.to_list(blob_mode="bytes") == [{"id": 1}]
    assert query.to_pandas(blob_mode="descriptions")["id"].tolist() == [1]
    with pytest.raises(ValueError, match="blob_mode must be one of"):
        query.to_list(blob_mode="handles")
    with pytest.raises(ValueError, match="blob_mode must be one of"):
        table.search().to_list(blob_mode="handles")


def test_take_queries_honor_blob_mode():
    table = _blob_table(
        "take_blob_mode", [{"id": 1, "image": b"one"}, {"id": 2, "image": b"two"}]
    )

    hits = table.take_offsets([1, 0]).to_arrow(blob_mode="bytes")
    assert dict(zip(hits["id"].to_pylist(), hits["image"].to_pylist())) == {
        1: b"one",
        2: b"two",
    }
    assert [
        row["image"] for row in table.take_row_ids([0]).to_list(blob_mode="bytes")
    ] == [b"one"]
    assert table.take_offsets([0]).to_pandas(blob_mode="bytes")["image"].tolist() == [
        b"one"
    ]
    description = table.take_row_ids([0]).to_list(blob_mode="descriptions")[0]["image"]
    assert "_lance_row_id" not in description
    batches = table.take_row_ids([0]).to_batches(blob_mode="bytes")
    assert batches.read_all().to_pylist() == [{"id": 1, "image": b"one"}]
    batches = table.take_row_ids([0]).to_batches(blob_mode="descriptions")
    assert "_rowid" not in batches.schema.names
    assert "_lance_row_id" not in str(batches.schema)
    assert batches.read_all().num_rows == 1


def test_bytes_mode_follows_swapped_blob_aliases():
    db = lancedb.connect("memory:///")
    table = db.create_table(
        "swapped_aliases",
        schema=pa.schema(
            [
                pa.field("id", pa.int64()),
                pa.field("caption", pa.utf8()),
                lancedb.blob("image"),
                lancedb.blob("thumb"),
            ]
        ),
    )
    table.add([{"id": 0, "caption": "cap", "image": b"img", "thumb": b"th"}])

    swapped = table.search().select({"id": "id", "image": "thumb", "thumb": "image"})
    assert swapped.to_list(blob_mode="bytes") == [
        {"id": 0, "image": b"th", "thumb": b"img"}
    ]
    over_text = table.search().select({"id": "id", "image": "caption", "pic": "image"})
    assert over_text.to_list(blob_mode="bytes") == [
        {"id": 0, "image": "cap", "pic": b"img"}
    ]


def test_bytes_mode_refused_under_namespace_query_pushdown(tmp_path):
    db = lancedb.connect_namespace(
        "dir",
        {"root": str(tmp_path)},
        namespace_client_pushdown_operations=["QueryTable"],
    )
    table = db.create_table(
        "pushdown_blob",
        schema=pa.schema([pa.field("id", pa.int64()), lancedb.blob("image")]),
    )
    table.add([{"id": 1, "image": b"one"}])

    with pytest.raises(NotImplementedError, match="stable table snapshot"):
        table.to_arrow(blob_mode="bytes")
    with pytest.raises(NotImplementedError, match="stable table snapshot"):
        table.search().to_list(blob_mode="bytes")
    with pytest.raises(NotImplementedError, match="stable table snapshot"):
        table.take_row_ids([0]).to_list(blob_mode="bytes")
    with pytest.raises(NotImplementedError, match="stable table snapshot"):
        table.search().to_batches(blob_mode="bytes")
    descriptor = table.to_arrow(blob_mode="descriptions")["image"].to_pylist()[0]
    assert "_lance_row_id" not in descriptor


@pytest.mark.asyncio
async def test_async_bytes_mode_refused_under_namespace_query_pushdown(tmp_path):
    db = lancedb.connect_namespace_async(
        "dir",
        {"root": str(tmp_path)},
        namespace_client_pushdown_operations=["QueryTable"],
    )
    table = await db.create_table(
        "pushdown_blob",
        schema=pa.schema([pa.field("id", pa.int64()), lancedb.blob("image")]),
    )
    await table.add([{"id": 1, "image": b"one"}])

    with pytest.raises(NotImplementedError, match="stable table snapshot"):
        await table.to_arrow(blob_mode="bytes")
    with pytest.raises(NotImplementedError, match="stable table snapshot"):
        await table.query().to_list(blob_mode="bytes")
    with pytest.raises(NotImplementedError, match="stable table snapshot"):
        await table.query().to_batches(blob_mode="bytes")


def test_fetch_blobs_round_trip():
    table = _blob_table(
        "round_trip",
        [{"id": 1, "image": b"alpha"}, {"id": 2, "image": b"beta"}],
    )
    by_id = _row_ids_by_id(table)
    blobs = table.fetch_blobs("image", [by_id[1], by_id[2]])
    assert [blobs[0].as_py(), blobs[1].as_py()] == [b"alpha", b"beta"]


def test_add_overwrite_preserves_blob_v2(tmp_path):
    db = lancedb.connect(tmp_path)
    schema = pa.schema([pa.field("id", pa.int64()), lancedb.blob("image")])
    table = db.create_table("overwrite_blob", schema=schema)
    table.add([{"id": 1, "image": b"old"}])

    payload = b"x" * 200_000
    table.add([{"id": 2, "image": payload}], mode="overwrite")

    assert table.count_rows() == 1
    assert blob_v2_column_paths(table.schema) == ["image"]
    assert table.blob_columns() == ["image"]
    assert table.fetch_blobs("image", [_row_ids_by_id(table)[2]]).to_pylist() == [
        payload
    ]
    assert list(tmp_path.rglob("*.blob"))


def test_add_overwrite_with_blob_still_infers_vector_schema():
    db = lancedb.connect("memory:///overwrite_blob_vector")
    schema = pa.schema(
        [
            pa.field("vector", pa.list_(pa.float32(), 2)),
            lancedb.blob("image"),
        ]
    )
    table = db.create_table("overwrite_blob_vector", schema=schema)

    table.add([{"vector": [1.0, 2.0, 3.0], "image": b"data"}], mode="overwrite")

    assert table.schema.field("vector").type == pa.list_(pa.float32(), 3)
    assert table.blob_columns() == ["image"]


@pytest.mark.asyncio
async def test_add_overwrite_preserves_blob_v2_async():
    db = await lancedb.connect_async("memory:///overwrite_blob_async")
    schema = pa.schema([pa.field("id", pa.int64()), lancedb.blob("image")])
    table = await db.create_table("overwrite_blob_async", schema=schema)

    await table.add([{"id": 1, "image": b"updated"}], mode="overwrite")

    assert blob_v2_column_paths(await table.schema()) == ["image"]
    assert await table.blob_columns() == ["image"]
    hits = await table.query().with_row_id().to_arrow()
    assert (await table.fetch_blobs("image", hits)).to_pylist() == [b"updated"]


def test_add_overwrite_preserves_nested_blob_v2():
    db = lancedb.connect("memory:///overwrite_nested_blob")
    schema = pa.schema(
        [
            pa.field("id", pa.int64()),
            pa.field("info", pa.struct([lancedb.blob("image")])),
        ]
    )
    table = db.create_table("overwrite_nested_blob", schema=schema)

    table.add([{"id": 1, "info": {"image": b"nested"}}], mode="overwrite")

    assert blob_v2_column_paths(table.schema) == ["info.image"]
    assert table.blob_columns() == ["info.image"]
    assert table.fetch_blobs("info.image", [_row_ids_by_id(table)[1]]).to_pylist() == [
        b"nested"
    ]


def test_merge_insert_writes_python_bytes():
    table = _blob_table("merge_bytes", [{"id": 1, "image": b"before"}])
    result = (
        table.merge_insert("id")
        .when_matched_update_all()
        .when_not_matched_insert_all()
        .execute([{"id": 1, "image": b"updated"}, {"id": 2, "image": b"inserted"}])
    )
    assert result.num_updated_rows == 1
    assert result.num_inserted_rows == 1
    by_id = _row_ids_by_id(table)
    blobs = table.fetch_blobs("image", [by_id[1], by_id[2]])
    assert blobs.to_pylist() == [b"updated", b"inserted"]


@pytest.mark.asyncio
@pytest.mark.parametrize("use_async", [False, True])
@pytest.mark.parametrize("input_type", ["extension", "struct", "reader"])
@pytest.mark.parametrize("target_with_ranges", [False, True])
async def test_merge_insert_normalizes_blob_layouts(
    use_async, input_type, target_with_ranges
):
    data = _blob_layout_data(
        [{"id": 1, "image": {"data": b"before"}}], target_with_ranges
    )
    table = lancedb.connect("memory:///").create_table("merge_layouts", data=data)
    original_schema = table.schema
    updates = _blob_layout_data(
        [
            {"id": 1, "image": {"data": b"updated"}},
            {"id": 2, "image": {"data": b"inserted"}},
            {"id": 3, "image": None},
        ],
        not target_with_ranges,
        as_extension=input_type != "struct",
    )
    if input_type == "reader":
        updates = updates.to_reader(max_chunksize=1)
    writer = table._table if use_async else table
    result = (
        writer.merge_insert("id")
        .when_matched_update_all()
        .when_not_matched_insert_all()
        .execute(updates)
    )
    if use_async:
        result = await result

    assert result.num_updated_rows == 1
    assert result.num_inserted_rows == 2
    assert table.schema == original_schema
    by_id = _row_ids_by_id(table)
    assert table.fetch_blobs("image", [by_id[i] for i in (1, 2, 3)]).to_pylist() == [
        b"updated",
        b"inserted",
        None,
    ]


@pytest.mark.asyncio
@pytest.mark.parametrize("use_async", [False, True])
@pytest.mark.parametrize(
    "byte_range", [{"position": 0}, {"size": 6}, {"position": 4, "size": 6}]
)
async def test_merge_insert_legacy_blob_rejects_ranges_atomically(
    use_async, byte_range
):
    data = _blob_layout_data([{"id": 1, "image": {"data": b"before"}}], False)
    table = lancedb.connect("memory:///").create_table("reject_ranges", data=data)
    original_schema, original_version = table.schema, table.version
    updates = _blob_layout_data(
        [
            {"id": 1, "image": {"data": b"updated"}},
            {"id": 2, "image": {"uri": "file:///payload", **byte_range}},
        ],
        True,
    ).to_reader(max_chunksize=1)
    writer = table._table if use_async else table
    with pytest.raises(RuntimeError, match="cannot store byte ranges"):
        result = (
            writer.merge_insert("id")
            .when_matched_update_all()
            .when_not_matched_insert_all()
            .execute(updates)
        )
        if use_async:
            await result

    assert table.version == original_version
    assert table.schema == original_schema
    assert table.count_rows() == 1
    assert table.fetch_blobs("image", [_row_ids_by_id(table)[1]]).to_pylist() == [
        b"before"
    ]


@pytest.mark.asyncio
@pytest.mark.parametrize("use_async", [False, True])
@pytest.mark.parametrize("input_type", ["extension", "struct", "reader"])
@pytest.mark.parametrize("source_with_ranges", [False, True])
async def test_add_overwrite_adopts_explicit_blob_layout(
    tmp_path, use_async, input_type, source_with_ranges
):
    data = _blob_layout_data(
        [{"id": 1, "image": {"data": b"before"}}], not source_with_ranges
    )
    table = lancedb.connect("memory:///").create_table("overwrite_layouts", data=data)
    payload = b"0123456789abcdef"
    path = tmp_path / "payload.bin"
    path.write_bytes(payload)
    value = (
        {"uri": path.as_uri(), "position": 4, "size": 6}
        if source_with_ranges
        else {"data": b"updated"}
    )
    updates = _blob_layout_data(
        [{"id": 2, "image": value}, {"id": 3, "image": None}],
        source_with_ranges,
        as_extension=input_type != "struct",
    )
    if input_type == "reader":
        updates = updates.to_reader(max_chunksize=1)
    writer = table._table if use_async else table
    result = writer.add(
        updates, mode="overwrite", allow_external_blob_outside_bases=True
    )
    if use_async:
        await result

    blob_type = table.schema.field("image").type
    if isinstance(blob_type, pa.ExtensionType):
        blob_type = blob_type.storage_type
    assert blob_type.num_fields == (4 if source_with_ranges else 2)
    assert table.blob_columns() == ["image"]
    assert table.count_rows() == 2
    by_id = _row_ids_by_id(table)
    assert table.fetch_blobs("image", [by_id[2], by_id[3]]).to_pylist() == [
        payload[4:10] if source_with_ranges else b"updated",
        None,
    ]


@pytest.mark.parametrize("write", ["merge_insert", "create_table"])
def test_uri_string_blob_write_reaches_external_base_validation(tmp_path, write):
    blob_path = tmp_path / "payload.bin"
    blob_path.write_bytes(b"payload")
    schema = pa.schema([pa.field("id", pa.int64()), lancedb.blob("image")])
    rows = pa.table(
        {"id": pa.array([1], pa.int64()), "image": pa.array([blob_path.as_uri()])}
    )
    db = lancedb.connect(tmp_path)

    # These APIs do not expose add's outside-base opt-in. The URI should reach
    # the blob policy check instead of failing at PyArrow's string-to-struct cast.
    with pytest.raises(ValueError, match="outside registered external bases"):
        if write == "merge_insert":
            table = db.create_table("merge_uri", schema=schema)
            table.merge_insert("id").when_not_matched_insert_all().execute(rows)
        else:
            db.create_table("create_uri", data=rows, schema=schema)


@pytest.mark.parametrize("write", ["merge_insert", "create_table"])
@pytest.mark.parametrize(
    "source_type, value",
    [
        (pa.binary(4), b"abcd"),
        (pa.list_(pa.uint8()), [1, 2]),
        (pa.int64(), 42),
    ],
)
def test_blob_write_rejects_unsupported_input(write, source_type, value):
    schema = pa.schema([pa.field("id", pa.int64()), lancedb.blob("image")])
    rows = pa.table(
        {"id": pa.array([1], pa.int64()), "image": pa.array([value], source_type)}
    )
    db = lancedb.connect("memory:///")

    with pytest.raises(ValueError, match="cannot coerce column 'image' with type"):
        if write == "merge_insert":
            table = db.create_table("merge_invalid", schema=schema)
            table.merge_insert("id").when_not_matched_insert_all().execute(rows)
        else:
            db.create_table("create_invalid", data=rows, schema=schema)


def test_merge_insert_bytes_after_reopen_without_touching_blob_type(tmp_path):
    db = lancedb.connect(tmp_path)
    schema = pa.schema([pa.field("id", pa.int64()), lancedb.blob("image")])
    table = db.create_table("images", schema=schema)
    table.add([{"id": 1, "image": b"hello"}])

    script = textwrap.dedent(
        f"""\
        import lancedb

        db = lancedb.connect({str(tmp_path)!r})
        table = db.open_table("images")
        image_type = table.schema.field("image").type
        if type(image_type).__name__ != "StructType":
            raise SystemExit(f"expected StructType, got {{type(image_type)}}")
        result = (
            table.merge_insert("id")
            .when_matched_update_all()
            .when_not_matched_insert_all()
            .execute(
                [{{"id": 1, "image": b"updated"}}, {{"id": 2, "image": b"inserted"}}]
            )
        )
        if result.num_updated_rows != 1 or result.num_inserted_rows != 1:
            raise SystemExit(
                f"rows updated={{result.num_updated_rows}} "
                f"inserted={{result.num_inserted_rows}}"
            )
        hits = table.search().with_row_id(True).limit(10).to_arrow()
        by_id = dict(zip(hits["id"].to_pylist(), hits["_rowid"].to_pylist()))
        blobs = table.fetch_blobs("image", [by_id[1], by_id[2]])
        if blobs.to_pylist() != [b"updated", b"inserted"]:
            raise SystemExit(blobs.to_pylist())
        """
    )
    result = subprocess.run(
        [sys.executable, "-c", script],
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, result.stderr


def test_merge_insert_bytes_after_reopen_without_pylance(tmp_path):
    db = lancedb.connect(tmp_path)
    schema = pa.schema([pa.field("id", pa.int64()), lancedb.blob("image")])
    table = db.create_table("images", schema=schema)
    table.add([{"id": 1, "image": b"hello"}])

    script = _HIDE_LANCE_BLOB + textwrap.dedent(
        f"""\
        import lancedb

        db = lancedb.connect({str(tmp_path)!r})
        table = db.open_table("images")
        image_type = table.schema.field("image").type
        if type(image_type).__name__ != "StructType":
            raise SystemExit(f"expected StructType, got {{type(image_type)}}")
        result = (
            table.merge_insert("id")
            .when_matched_update_all()
            .when_not_matched_insert_all()
            .execute(
                [{{"id": 1, "image": b"updated"}}, {{"id": 2, "image": b"inserted"}}]
            )
        )
        if result.num_updated_rows != 1 or result.num_inserted_rows != 1:
            raise SystemExit(
                f"rows updated={{result.num_updated_rows}} "
                f"inserted={{result.num_inserted_rows}}"
            )
        hits = table.search().with_row_id(True).limit(10).to_arrow()
        by_id = dict(zip(hits["id"].to_pylist(), hits["_rowid"].to_pylist()))
        blobs = table.fetch_blobs("image", [by_id[1], by_id[2]])
        if blobs.to_pylist() != [b"updated", b"inserted"]:
            raise SystemExit(blobs.to_pylist())
        """
    )
    result = subprocess.run(
        [sys.executable, "-c", script],
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, result.stderr


def test_merge_insert_blob_array_into_reopened_unregistered_table(tmp_path):
    db = lancedb.connect(tmp_path)
    schema = pa.schema([pa.field("id", pa.int64()), lancedb.blob("image")])
    table = db.create_table("images", schema=schema)
    table.add([{"id": 1, "image": b"before"}])

    script = textwrap.dedent(
        f"""\
        import pyarrow as pa
        import lancedb

        db = lancedb.connect({str(tmp_path)!r})
        table = db.open_table("images")
        image_type = table.schema.field("image").type
        if type(image_type).__name__ != "StructType":
            raise SystemExit(
                f"expected StructType before lance import, got {{type(image_type)}}"
            )

        import lance

        updates = pa.Table.from_arrays(
            [
                pa.array([1, 2], type=pa.int64()),
                lance.blob_array([b"updated", b"inserted"]),
            ],
            names=["id", "image"],
        )
        result = (
            table.merge_insert("id")
            .when_matched_update_all()
            .when_not_matched_insert_all()
            .execute(updates)
        )
        if result.num_updated_rows != 1 or result.num_inserted_rows != 1:
            raise SystemExit(
                f"rows updated={{result.num_updated_rows}} "
                f"inserted={{result.num_inserted_rows}}"
            )
        hits = table.search().with_row_id(True).limit(10).to_arrow()
        by_id = dict(zip(hits["id"].to_pylist(), hits["_rowid"].to_pylist()))
        blobs = table.fetch_blobs("image", [by_id[1], by_id[2]])
        if blobs.to_pylist() != [b"updated", b"inserted"]:
            raise SystemExit(blobs.to_pylist())
        """
    )
    result = subprocess.run(
        [sys.executable, "-c", script],
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, result.stderr


def test_add_all_null_blob_column():
    db = lancedb.connect("memory:///")
    schema = pa.schema([pa.field("id", pa.int64()), lancedb.blob("image")])
    table = db.create_table("all_null", schema=schema)
    table.add([{"id": 1, "image": None}, {"id": 2, "image": None}])
    by_id = _row_ids_by_id(table)
    blobs = table.fetch_blobs("image", [by_id[1], by_id[2]])
    assert blobs.to_pylist() == [None, None]


def test_create_table_nested_blob_schema_without_rows():
    db = lancedb.connect("memory:///")
    schema = pa.schema(
        [
            pa.field("id", pa.int64()),
            pa.field("info", pa.struct([lancedb.blob("blob")])),
            pa.field("images", pa.list_(lancedb.blob("image"))),
        ]
    )
    table = db.create_table("nested_empty", schema=schema)
    assert table.count_rows() == 0


def test_merge_insert_nested_blob_dicts():
    db = lancedb.connect("memory:///")
    info = pa.StructArray.from_arrays(
        [
            pa.array(["first"], type=pa.string()),
            _blob_array("blob", [b"before"]),
        ],
        names=["name", "blob"],
    )
    data = pa.Table.from_arrays(
        [pa.array([1], type=pa.int64()), info],
        names=["id", "info"],
    )
    table = db.create_table("nested_merge", data=data)
    result = (
        table.merge_insert("id")
        .when_matched_update_all()
        .execute([{"id": 1, "info": {"name": "first", "blob": b"after"}}])
    )
    assert result.num_updated_rows == 1
    by_id = _row_ids_by_id(table)
    blobs = table.fetch_blobs("info.blob", [by_id[1]])
    assert blobs.to_pylist() == [b"after"]


def _list_blob_table(name):
    db = lancedb.connect("memory:///")
    blob_field = lancedb.blob("image")
    images = pa.ListArray.from_arrays(
        pa.array([0, 1], type=pa.int32()), _blob_array("image", [b"before"])
    )
    data = pa.Table.from_arrays(
        [pa.array([1], type=pa.int64()), images],
        schema=pa.schema(
            [pa.field("id", pa.int64()), pa.field("images", pa.list_(blob_field))]
        ),
    )
    return db.create_table(name, data=data)


def test_merge_insert_list_blob_dicts():
    table = _list_blob_table("list_merge")
    result = (
        table.merge_insert("id")
        .when_matched_update_all()
        .when_not_matched_insert_all()
        .execute([{"id": 1, "images": [b"one", b"two"]}, {"id": 2, "images": None}])
    )
    assert result.num_updated_rows == 1
    assert result.num_inserted_rows == 1
    hits = table.search().limit(10).to_arrow()
    sizes = {
        row["id"]: None if row["images"] is None else [d["size"] for d in row["images"]]
        for row in hits.to_pylist()
    }
    assert sizes == {1: [3, 3], 2: None}


def test_list_blob_column_queries_as_raw_descriptors():
    table = _list_blob_table("list_query")
    assert table.blob_columns() == []
    hits = table.search().limit(10).to_arrow()
    element = hits.schema.field("images").type.value_type
    assert pa.types.is_struct(element)
    assert "_lance_row_id" not in element.names
    for column in ("images", "images.image"):
        with pytest.raises(ValueError, match="blobs inside lists cannot be fetched"):
            table.fetch_blobs(column, [0])
        with pytest.raises(ValueError, match="blobs inside lists cannot be fetched"):
            table.fetch_blob_files(column, [0])
        with pytest.raises(ValueError, match="blobs inside lists cannot be fetched"):
            table.fetch_blob_ranges(column, [(0, 0, 1)])


def test_bytes_mode_rejects_blobs_inside_lists():
    table = _list_blob_table("list_blob_bytes")

    with pytest.raises(ValueError, match=r"\['images.image'\].*blobs inside lists"):
        table.to_arrow(blob_mode="bytes")
    with pytest.raises(ValueError, match="blobs inside lists"):
        table.search().select({"pics": "images"}).to_list(blob_mode="bytes")
    descriptions = table.to_arrow(blob_mode="descriptions")["images"].to_pylist()
    assert descriptions[0][0]["size"] == len(b"before")


def test_row_addressable_paths_exclude_list_children():
    from lancedb.schema import row_addressable_blob_v2_paths

    schema = pa.schema(
        [
            pa.field("id", pa.int64()),
            pa.field("info", pa.struct([lancedb.blob("blob")])),
            pa.field("images", pa.list_(lancedb.blob("image"))),
        ]
    )
    assert blob_v2_column_paths(schema) == ["info.blob", "images.image"]
    assert row_addressable_blob_v2_paths(schema) == ["info.blob"]


def test_merge_insert_writes_pylance_blob_array():
    table = _blob_table("merge_pylance", [{"id": 1, "image": b"before"}])
    image = lance.blob_array([b"updated", b"inserted"])
    assert type(image.type) is LanceBlobType
    assert type(image.type) is type(lancedb.BlobType())
    updates = pa.Table.from_arrays(
        [pa.array([1, 2], type=pa.int64()), image], names=["id", "image"]
    )

    result = (
        table.merge_insert("id")
        .when_matched_update_all()
        .when_not_matched_insert_all()
        .execute(updates)
    )

    assert result.num_updated_rows == 1
    assert result.num_inserted_rows == 1
    by_id = _row_ids_by_id(table)
    blobs = table.fetch_blobs("image", [by_id[1], by_id[2]])
    assert blobs.to_pylist() == [b"updated", b"inserted"]


def test_fetch_blobs_accepts_query_result():
    table = _blob_table("from_result", [{"id": 1, "image": b"gamma"}])
    hits = table.search().limit(10).to_arrow()
    assert "_rowid" not in hits.column_names
    blobs = table.fetch_blobs("image", hits)
    assert {blobs[i].as_py() for i in range(len(blobs))} == {b"gamma"}


def test_fetch_blob_apis_accept_integer_arrow_row_ids():
    table = _blob_table(
        "arrow_row_ids",
        [{"id": 1, "image": b"alpha"}, {"id": 2, "image": b"beta"}],
    )
    by_id = _row_ids_by_id(table)
    ids = [by_id[2], by_id[1]]

    for row_ids in (
        pa.array(ids, type=pa.uint64()),
        pa.chunked_array([ids[:1], ids[1:]], type=pa.int64()),
    ):
        assert table.fetch_blobs("image", row_ids).to_pylist() == [b"beta", b"alpha"]
        files = table.fetch_blob_files("image", row_ids)
        assert [file.read() for file in files] == [b"beta", b"alpha"]


def test_fetch_blob_apis_validate_arrow_and_negative_row_ids():
    table = _blob_table("invalid_arrow_row_ids", [{"id": 1, "image": b"alpha"}])
    hits = table.search().with_row_id(True).to_arrow()

    for fetch in (table.fetch_blobs, table.fetch_blob_files):
        with pytest.raises(ValueError, match="non-negative"):
            fetch("image", [-1])
        with pytest.raises(ValueError, match="non-negative"):
            fetch("image", pa.array([-1], type=pa.int64()))
        with pytest.raises(ValueError, match="without nulls"):
            fetch("image", pa.array([None], type=pa.int64()))
        with pytest.raises(ValueError, match="integer type"):
            fetch("image", hits["image"])


def test_fetch_blobs_after_compact_with_stable_row_ids(tmp_path):
    db = lancedb.connect(
        tmp_path, storage_options={"new_table_enable_stable_row_ids": "true"}
    )
    schema = pa.schema([pa.field("id", pa.int64()), lancedb.blob("image")])
    table = db.create_table("t", schema=schema)
    table.add([{"id": 1, "image": b"frag-one"}])
    table.add([{"id": 2, "image": b"frag-two"}])
    by_id = _row_ids_by_id(table)
    ids = [by_id[1], by_id[2]]

    table.optimize()

    blobs = table.fetch_blobs("image", ids)
    assert blobs.to_pylist() == [b"frag-one", b"frag-two"]
    ranges = table.fetch_blob_ranges("image", [(ids[0], 5, 3), (ids[1], 5, 3)])
    assert ranges.to_pylist() == [b"one", b"two"]


def test_fetch_blobs_preserves_null_and_empty_values():
    table = _blob_table(
        "nulls",
        [
            {"id": 1, "image": b"present"},
            {"id": 2, "image": None},
            {"id": 3, "image": b""},
        ],
    )
    by_id = _row_ids_by_id(table)
    request = [by_id[1], by_id[2], by_id[3], by_id[1]]
    blobs = table.fetch_blobs("image", request)
    assert len(blobs) == len(request)
    assert blobs[0].as_py() == b"present"
    assert blobs[1].as_py() is None
    assert blobs[2].as_py() == b""
    assert blobs[3].as_py() == b"present"


def test_add_all_null_list_to_blob_column():
    table = _blob_table("all_null_add", [{"id": 1, "image": None}])

    hits = table.search().to_arrow()
    blobs = table.fetch_blobs("image", hits)
    assert len(blobs) == 1
    assert blobs[0].as_py() is None


def test_add_all_null_list_to_blob_column_with_sanitizer():
    db = lancedb.connect("memory:///")
    schema = pa.schema([pa.field("id", pa.int64()), lancedb.blob("image")])
    table = db.create_table("all_null_sanitized_add", schema=schema)

    table.add([{"id": 1, "image": None}], on_bad_vectors="fill")

    hits = table.search().to_arrow()
    blobs = table.fetch_blobs("image", hits)
    assert len(blobs) == 1
    assert blobs[0].as_py() is None


def test_add_all_null_list_to_nested_blob_column():
    db = lancedb.connect("memory:///")
    blob_field = lancedb.blob("image")
    info_field = pa.field("info", pa.struct([blob_field]))
    info = pa.StructArray.from_arrays(
        [_blob_array("image", [b"seed"])], fields=[blob_field]
    )
    seed = pa.Table.from_arrays(
        [pa.array([0], type=pa.int64()), info],
        schema=pa.schema([pa.field("id", pa.int64()), info_field]),
    )
    table = db.create_table("nested_null_add", data=seed)

    table.add([{"id": 1, "info": {"image": None}}])
    table.add([{"id": 2, "info": {"image": None}}], on_bad_vectors="fill")

    hits = table.search().where("id > 0").to_arrow()
    blobs = table.fetch_blobs("info.image", hits)
    assert len(blobs) == 2
    assert all(blob.as_py() is None for blob in blobs)


@pytest.mark.parametrize("large_list", [False, True], ids=["list", "large_list"])
def test_add_list_of_dicts_to_blob_list_column(large_list):
    db = lancedb.connect("memory:///")
    blob_field = lancedb.blob("image")
    blob_values = _blob_array("image", [b"seed"])
    if large_list:
        items_field = pa.field("items", pa.large_list(blob_field))
        items = pa.LargeListArray.from_arrays(
            pa.array([0, 1], type=pa.int64()), blob_values
        )
    else:
        items_field = pa.field("items", pa.list_(blob_field))
        items = pa.ListArray.from_arrays(pa.array([0, 1], type=pa.int32()), blob_values)
    seed = pa.Table.from_arrays(
        [pa.array([0], type=pa.int64()), items],
        schema=pa.schema([pa.field("id", pa.int64()), items_field]),
    )
    table = db.create_table(f"blob_{large_list}_list_add", data=seed)

    table.add([{"id": 1, "items": [None]}])
    table.add([{"id": 2, "items": [b"a", None, b""]}])
    table.add(
        [{"id": 3, "items": [b"b", None]}],
        on_bad_vectors="fill",
    )

    ids = table.search().select(["id"]).to_arrow()["id"].to_pylist()
    assert sorted(ids) == [0, 1, 2, 3]
    assert pa.types.is_large_list(table.schema.field("items").type) is large_list


def test_fetch_blob_ranges_aligns_repeated_ranges_and_nulls():
    table = _blob_table(
        "range_alignment",
        [{"id": 1, "image": b"abcdefghij"}, {"id": 2, "image": None}],
    )
    by_id = _row_ids_by_id(table)
    requests = [
        (by_id[1], 2, 3),
        (by_id[2], 0, 0),
        (by_id[1], 0, 2),
        (by_id[1], 2, 3),
        (by_id[1], 10, 0),
    ]

    ranges = table.fetch_blob_ranges("image", requests)

    assert ranges.to_pylist() == [b"cde", None, b"ab", b"cde", b""]


def test_fetch_blob_ranges_accepts_three_item_sequences():
    table = _blob_table("range_sequences", [{"id": 1, "image": b"abcdef"}])
    row_id = _row_ids_by_id(table)[1]

    ranges = table.fetch_blob_ranges("image", [[row_id, 1, 2], (row_id, 4, 2)])

    assert ranges.to_pylist() == [b"bc", b"ef"]


def test_fetch_blob_ranges_validates_requests():
    table = _blob_table("range_validation", [{"id": 1, "image": b"abc"}])
    row_id = _row_ids_by_id(table)[1]

    with pytest.raises(ValueError, match="exceeds blob size"):
        table.fetch_blob_ranges("image", [(row_id, 2, 2)])

    with pytest.raises(ValueError, match="offset \\+ length overflowed"):
        table.fetch_blob_ranges("image", [(row_id, 2**64 - 1, 1)])

    with pytest.raises(ValueError) as exc_info:
        table.fetch_blob_ranges("image", [(2**64 - 1, 0, 1)])
    _assert_missing_blob_row_ids(exc_info)

    for index, name in enumerate(("row id", "offset", "length")):
        request = [row_id, 0, 1]
        request[index] = -1
        with pytest.raises(
            ValueError, match=f"blob range {name} must be a non-negative"
        ):
            table.fetch_blob_ranges("image", [request])

    with pytest.raises(ValueError, match="three values"):
        table.fetch_blob_ranges("image", [[row_id, 0]])


def test_fetch_blob_apis_reject_row_past_fragment_end():
    table = _blob_table(
        "past_fragment_end",
        [
            {"id": 1, "image": b"a"},
            {"id": 2, "image": b"b"},
            {"id": 3, "image": b"c"},
        ],
    )
    _assert_fetch_apis_reject_missing_row_ids(table, [3])
    _assert_fetch_apis_reject_missing_row_ids(table, [3, 0])


def test_fetch_blob_apis_reject_missing_fragment_row_addr():
    table = _blob_table("missing_frag", [{"id": 1, "image": b"x"}])
    live = _row_ids_by_id(table)[1]
    _assert_fetch_apis_reject_missing_row_ids(table, [1 << 32, live])


def test_fetch_blob_apis_reject_deleted_row_ids():
    table = _blob_table(
        "deleted_rows",
        [{"id": 1, "image": b"one"}, {"id": 2, "image": b"two"}],
    )
    by_id = _row_ids_by_id(table)
    table.delete("id = 2")
    _assert_fetch_apis_reject_missing_row_ids(table, [by_id[2], by_id[1]])


def test_fetch_blob_apis_reject_deleted_stable_row_ids(tmp_path):
    db = lancedb.connect(
        tmp_path, storage_options={"new_table_enable_stable_row_ids": "true"}
    )
    schema = pa.schema([pa.field("id", pa.int64()), lancedb.blob("image")])
    table = db.create_table("stable_deleted", schema=schema)
    table.add([{"id": 1, "image": b"one"}, {"id": 2, "image": b"two"}])
    by_id = _row_ids_by_id(table)
    table.delete("id = 2")

    _assert_fetch_apis_reject_missing_row_ids(table, [by_id[2], by_id[1]])


def test_fetch_blob_ranges_empty_requests_returns_empty_array():
    table = _blob_table("range_empty", [{"id": 1, "image": b"x"}])
    assert table.fetch_blob_ranges("image", []).to_pylist() == []


@pytest.mark.asyncio
async def test_async_fetch_blob_ranges():
    db = await lancedb.connect_async("memory:///")
    schema = pa.schema([pa.field("id", pa.int64()), lancedb.blob("image")])
    table = await db.create_table("range_async", schema=schema)
    await table.add([{"id": 1, "image": b"abcdefghij"}])
    hits = await table.query().with_row_id().to_arrow()
    row_id = hits["_rowid"][0].as_py()

    ranges = await table.fetch_blob_ranges("image", [(row_id, 1, 3), (row_id, 6, 2)])

    assert ranges.to_pylist() == [b"bcd", b"gh"]


def test_fetch_blobs_nested_path():
    db = lancedb.connect("memory:///")
    info = pa.StructArray.from_arrays(
        [
            pa.array(["first", "second"], type=pa.string()),
            _blob_array("blob", [b"nested-alpha", b"nested-beta"]),
        ],
        names=["name", "blob"],
    )
    data = pa.Table.from_arrays(
        [pa.array([1, 2], type=pa.int64()), info],
        names=["id", "info"],
    )
    table = db.create_table("nested", data=data)

    by_id = _row_ids_by_id(table)
    blobs = table.fetch_blobs("info.blob", [by_id[1], by_id[2]])
    assert [blobs[0].as_py(), blobs[1].as_py()] == [b"nested-alpha", b"nested-beta"]

    # The source path's parent name now holds a string column in the hits.
    hits = table.search().select({"id": "id", "info": "info.name", "b": "info.blob"})
    blobs = table.fetch_blobs("info.blob", hits.to_arrow())
    assert blobs.to_pylist() == [b"nested-alpha", b"nested-beta"]


def test_fetch_blob_files_lazy_read():
    payload = b"lazy-read" * 100
    table = _blob_table("lazy", [{"id": 1, "image": payload}])
    by_id = _row_ids_by_id(table)
    handles = table.fetch_blob_files("image", [by_id[1]])
    assert len(handles) == 1
    assert handles[0].read() == payload
    handles[0].close()
    assert handles[0].closed
    with pytest.raises(ValueError, match="read of closed file"):
        handles[0].read()


def test_blob_file_finalizer_and_close_on_runtime_worker():
    # A tokio panic in IOBase.__del__ is printed but does not fail the add call.
    # Run in a child process so the assertion catches that panic on stderr.
    script = textwrap.dedent(
        """\
        import gc
        import threading

        import lancedb
        import pyarrow as pa

        db = lancedb.connect("memory:///")
        schema = pa.schema([pa.field("id", pa.int64()), lancedb.blob("image")])
        table = db.create_table("finalize", schema=schema)
        table.add([{"id": 1, "image": b"first"}])
        hits = table.search().with_row_id(True).limit(1).to_arrow()
        row_id = hits["_rowid"][0].as_py()
        gc_handle, explicit_handle = table.fetch_blob_files("image", [row_id, row_id])

        gc.disable()
        cycle = [gc_handle]
        cycle.append(cycle)
        del gc_handle, cycle

        observed = []
        def on_progress(_):
            if not observed:
                observed.append((
                    threading.current_thread() is not threading.main_thread(),
                    explicit_handle.closed,
                    gc.collect(),
                ))
                explicit_handle.close()

        table.add([{"id": 2, "image": b"second"}], progress=on_progress)
        gc.enable()

        assert len(observed) == 1, observed
        assert observed[0][0] is True, observed
        assert observed[0][1] is False, observed
        assert observed[0][2] > 0, observed
        assert explicit_handle.closed
        explicit_handle.close()  # Close remains idempotent after the worker call.
        try:
            explicit_handle.read()
        except ValueError as error:
            assert "read of closed file" in str(error)
        else:
            raise AssertionError("a closed blob file was readable")
        print("worker close completed")
        """
    )
    result = subprocess.run(
        [sys.executable, "-c", script], capture_output=True, text=True, check=False
    )
    assert result.returncode == 0, result.stderr
    assert "Cannot start a runtime from within a runtime" not in result.stderr
    assert "worker close completed" in result.stdout


def test_fetch_blob_files_null_alignment():
    table = _blob_table(
        "lazy_nulls",
        [{"id": 1, "image": b"here"}, {"id": 2, "image": None}],
    )
    by_id = _row_ids_by_id(table)
    handles = table.fetch_blob_files("image", [by_id[2], by_id[1]])
    assert len(handles) == 2
    assert handles[0] is None
    assert handles[1].read() == b"here"


def test_fetch_blobs_rejects_non_blob_column():
    table = _blob_table("reject", [{"id": 1, "image": b"x"}])
    with pytest.raises(ValueError, match="not a blob column"):
        table.fetch_blobs("id", [0])


def test_legacy_v1_query_omits_auto_row_id():
    table = _legacy_v1_table("legacy_v1")
    hits = table.search().select(["legacy"]).limit(10).to_arrow()
    assert "_rowid" not in hits.column_names


def test_fetch_legacy_v1_blobs():
    table = _legacy_v1_table("legacy_fetch")
    table.add(
        [
            {"id": 2, "legacy": None},
            {"id": 3, "legacy": b""},
            {"id": 4, "legacy": b"another fragment"},
        ]
    )
    hits = table.search().select(["id", "legacy"]).with_row_id(True).to_arrow()
    assert pa.types.is_struct(hits["legacy"].type)
    by_id = dict(zip(hits["id"].to_pylist(), hits["_rowid"].to_pylist()))
    row_ids = [by_id[i] for i in [4, 2, 1, 3, 4]]
    expected = [b"another fragment", None, b"old", b"", b"another fragment"]
    assert table.fetch_blobs("legacy", row_ids).to_pylist() == expected
    assert table.blob_columns() == ["legacy"]
    assert table.fetch_blobs("legacy", hits).to_pylist() == [
        b"old",
        None,
        b"",
        b"another fragment",
    ]
    assert table.fetch_blobs("legacy", []).to_pylist() == []
    assert table.fetch_blob_files("legacy", []) == []

    handles = table.fetch_blob_files("legacy", row_ids)
    assert len(handles) == len(expected)
    for handle, payload in zip(handles, expected):
        if payload is None:
            assert handle is None
        else:
            with handle:
                assert handle.size() == len(payload)
                assert handle.read(2) == payload[:2]
                assert handle.seek(0) == 0
                assert handle.read() == payload

    assert table.fetch_blob_ranges(
        "legacy", [(by_id[4], 8, 8), (by_id[2], 0, 1), (by_id[3], 0, 0)]
    ).to_pylist() == [b"fragment", None, b""]

    table.delete("id = 2")
    for fetch in [table.fetch_blobs, table.fetch_blob_files]:
        with pytest.raises(ValueError, match="row ids but some do not exist"):
            fetch("legacy", [by_id[1], by_id[2]])


@pytest.mark.asyncio
async def test_async_fetch_legacy_v1_blobs(tmp_path):
    table = _legacy_v1_table("legacy_async_source")
    # Open a dataset written by Lance independently of the LanceDB connection.
    lance.write_dataset(
        pa.Table.from_pylist(
            [{"id": 1, "legacy": b"old"}, {"id": 2, "legacy": None}],
            schema=table.schema,
        ),
        tmp_path / "legacy.lance",
        data_storage_version="2.0",
    )
    db = await lancedb.connect_async(tmp_path)
    table = await db.open_table("legacy")
    assert await table.blob_columns() == ["legacy"]
    hits = await table.query().with_row_id().to_arrow()
    assert (await table.fetch_blobs("legacy", hits)).to_pylist() == [b"old", None]
    handles = await table.fetch_blob_files("legacy", hits)
    assert handles[0].read() == b"old"
    handles[0].close()
    assert handles[1] is None


@pytest.mark.asyncio
async def test_async_fetch_blob_files_lazy_read():
    db = await lancedb.connect_async("memory:///")
    schema = pa.schema([pa.field("id", pa.int64()), lancedb.blob("image")])
    table = await db.create_table("async_lazy", schema=schema)
    payload = b"async-lazy" * 100
    await table.add([{"id": 1, "image": payload}])
    hits = (
        await table.query().select({"image_alias": "image"}).limit(10).to_arrow()
    ).combine_chunks()
    assert "_rowid" not in hits.column_names
    handles = await table.fetch_blob_files("image", hits)
    assert len(handles) == 1
    assert await handles[0].aread() == payload
    handles[0].close()
    with pytest.raises(ValueError, match="read of closed file"):
        await handles[0].aread()


def test_fetch_blobs_from_query_result_without_row_id_raises():
    table = _blob_table("no_rowid", [{"id": 1, "image": b"x"}])
    hits = table.search().select(["id"]).to_arrow()
    assert "_rowid" not in hits.column_names
    with pytest.raises(ValueError, match="_rowid"):
        table.fetch_blobs("image", hits)


_HYBRID_BLOB_SCHEMA = pa.schema(
    [
        pa.field("id", pa.int64()),
        pa.field("text", pa.utf8()),
        pa.field("vector", pa.list_(pa.float32(), list_size=2)),
        lancedb.blob("image"),
    ]
)
_HYBRID_BLOB_ROWS = [
    {"id": 1, "text": "hello alpha", "vector": [1.0, 0.0], "image": b"alpha"},
    {"id": 2, "text": "hello beta", "vector": [0.9, 0.1], "image": b"beta"},
    {"id": 3, "text": "other", "vector": [0.0, 1.0], "image": b"other"},
]


def _hybrid_blob_table(db):
    table = db.create_table("hybrid_blob_fetch", schema=_HYBRID_BLOB_SCHEMA)
    table.add(_HYBRID_BLOB_ROWS)
    table.create_index("text", config=FTS(with_position=False))
    return table


async def _hybrid_blob_table_async(db):
    table = await db.create_table("hybrid_blob_fetch_async", schema=_HYBRID_BLOB_SCHEMA)
    await table.add(_HYBRID_BLOB_ROWS)
    await table.create_index("text", config=FTS(with_position=False))
    return table


def test_blob_v2_hybrid_fetch_blobs():
    table = _hybrid_blob_table(lancedb.connect("memory:///"))
    hits = (
        table.search(query_type="hybrid")
        .vector([1.0, 0.0])
        .text("hello")
        .select(["id", "image"])
        .limit(2)
        .to_arrow()
    )

    assert "_rowid" not in hits.column_names
    assert "_lance_row_id" in hits.schema.field("image").type.names
    blobs = table.fetch_blobs("image", hits)
    assert {blobs[i].as_py() for i in range(len(blobs))} == {b"alpha", b"beta"}


def _blob_query_shape(table, shape):
    if shape == "scan":
        return table.search().where("id < 3")
    if shape == "vector":
        return table.search([1.0, 0.0])
    if shape == "fts":
        return table.search("hello", query_type="fts")
    return table.search(query_type="hybrid").vector([1.0, 0.0]).text("hello")


@pytest.mark.parametrize("with_row_id", [None, True, False])
@pytest.mark.parametrize("blob_mode", ["bytes", "descriptions"])
@pytest.mark.parametrize("shape", ["scan", "vector", "fts", "hybrid"])
def test_blob_mode_for_every_query_shape(shape, blob_mode, with_row_id):
    table = _hybrid_blob_table(lancedb.connect("memory:///"))
    query = _blob_query_shape(table, shape).select(["id", "image"]).limit(2)
    if with_row_id is not None:
        query = query.with_row_id(with_row_id)

    hits = query.to_arrow(blob_mode=blob_mode)

    assert ("_rowid" in hits.column_names) == (with_row_id is True)
    images = {row["id"]: row["image"] for row in hits.to_pylist()}
    if blob_mode == "bytes":
        assert images == {1: b"alpha", 2: b"beta"}
    else:
        assert sorted(images) == [1, 2]
        assert all("_lance_row_id" not in image for image in images.values())
    df = query.to_pandas(blob_mode=blob_mode)
    assert dict(zip(df["id"], df["image"])) == images
    if shape in ("scan", "vector"):
        reader = query.to_batches(batch_size=1, blob_mode=blob_mode)
        assert reader.schema == hits.schema
        assert reader.read_all().equals(hits)


@pytest.mark.asyncio
@pytest.mark.parametrize("shape", ["scan", "vector", "fts", "hybrid"])
async def test_async_blob_bytes_for_every_query_shape(shape):
    db = await lancedb.connect_async("memory:///")
    table = await _hybrid_blob_table_async(db)
    query = table.query()
    if shape in ("vector", "hybrid"):
        query = query.nearest_to([1.0, 0.0])
    if shape in ("fts", "hybrid"):
        query = query.nearest_to_text("hello")
    if shape == "scan":
        query = query.where("id < 3")
    query = query.select(["id", "image"]).limit(2)

    hits = await query.to_arrow(blob_mode="bytes")

    assert {row["id"]: row["image"] for row in hits.to_pylist()} == {
        1: b"alpha",
        2: b"beta",
    }
    df = await query.to_pandas(blob_mode="bytes")
    assert dict(zip(df["id"], df["image"])) == {1: b"alpha", 2: b"beta"}


@pytest.mark.asyncio
@pytest.mark.parametrize("shape", ["scan", "vector", "fts", "hybrid"])
@pytest.mark.parametrize("blob_mode", ["lazy", "descriptions", "bytes"])
async def test_async_to_batches_blob_modes(shape, blob_mode, monkeypatch):
    db = await lancedb.connect_async("memory:///")
    table = await _hybrid_blob_table_async(db)
    query = table.query()
    if shape in ("vector", "hybrid"):
        query = query.nearest_to([1.0, 0.0])
    if shape in ("fts", "hybrid"):
        query = query.nearest_to_text("hello")
    if shape == "scan":
        query = query.where("id < 3")
    query = query.select({"id": "id", "payload": "image"}).limit(2)
    fetched = []
    fetch_blobs = table.fetch_blobs

    async def fetch(column, row_ids):
        fetched.append(row_ids)
        return await fetch_blobs(column, row_ids)

    monkeypatch.setattr(table, "fetch_blobs", fetch)
    reader = await query.to_batches(max_batch_length=1, blob_mode=blob_mode)
    assert fetched == []
    first = await reader.__anext__()
    assert len(fetched) == (1 if blob_mode == "bytes" else 0)
    batches = [first, *[batch async for batch in reader]]
    assert all(batch.num_rows == 1 for batch in batches)
    assert all(batch.schema == reader.schema for batch in batches)
    if blob_mode == "bytes":
        assert len(fetched) == 2
        assert {
            row["id"]: row["payload"] for b in batches for row in b.to_pylist()
        } == {
            1: b"alpha",
            2: b"beta",
        }
    if blob_mode != "lazy":
        assert "_rowid" not in reader.schema.names
        assert "_lance_row_id" not in str(reader.schema)


class _KeepAllScoresReranker(Reranker):
    """Rebuilds the merged table like model rerankers with return_score="all"."""

    def __init__(self):
        super().__init__(return_score="all")

    def rerank_hybrid(self, query, vector_results, fts_results):
        combined = self._merge_and_keep_scores(vector_results, fts_results)
        scores = pa.array([1.0] * len(combined), type=pa.float32())
        return combined.append_column("_relevance_score", scores)


def test_bytes_mode_rejects_legacy_blob_after_reranker_drops_metadata(tmp_path):
    db = lancedb.connect(tmp_path)
    table = db.create_table(
        "legacy_rerank",
        pa.table(
            {
                "id": pa.array([1, 2], pa.int64()),
                "text": ["hello alpha", "hello beta"],
                "vector": pa.array(
                    [[1.0, 0.0], [0.9, 0.1]], type=pa.list_(pa.float32(), 2)
                ),
                "blob": pa.array([b"one", b"two"], pa.large_binary()),
            },
            schema=_HYBRID_BLOB_SCHEMA.remove(3).append(
                pa.field(
                    "blob", pa.large_binary(), metadata={"lance-encoding:blob": "true"}
                )
            ),
        ),
        storage_options={"new_table_data_storage_version": "2.1"},
    )
    table.create_index("text", config=FTS(with_position=False))
    query = (
        table.search(query_type="hybrid")
        .vector([1.0, 0.0])
        .text("hello")
        .rerank(_KeepAllScoresReranker())
    )

    with pytest.raises(ValueError, match=r"\['blob'\].*legacy blob columns"):
        query.to_arrow(blob_mode="bytes")


@pytest.mark.parametrize("source", ["`image`", "IMAGE"])
def test_bytes_mode_rejects_blob_spelling_after_reranker_drops_metadata(source):
    table = _hybrid_blob_table(lancedb.connect("memory:///"))
    query = (
        table.search(query_type="hybrid")
        .vector([1.0, 0.0])
        .text("hello")
        .select({"id": "id", "pic": source})
        .rerank(_KeepAllScoresReranker())
    )

    with pytest.raises(ValueError, match=r"could not match result columns \['pic'\]"):
        query.to_arrow(blob_mode="bytes")


@pytest.mark.asyncio
async def test_blob_v2_hybrid_fetch_blobs_async():
    db = await lancedb.connect_async("memory:///hybrid_blob_fetch_async")
    table = await _hybrid_blob_table_async(db)
    hits = await (
        table.query()
        .nearest_to([1.0, 0.0])
        .nearest_to_text("hello")
        .select(["id", "image"])
        .limit(2)
        .to_arrow()
    )

    assert "_rowid" not in hits.column_names
    assert "_lance_row_id" in hits.schema.field("image").type.names
    blobs = await table.fetch_blobs("image", hits)
    assert {blobs[i].as_py() for i in range(len(blobs))} == {b"alpha", b"beta"}


@pytest.mark.asyncio
async def test_async_hybrid_typed_blob_projection_preserves_source_column():
    db = await lancedb.connect_async("memory:///hybrid_typed_blob")
    schema = pa.schema(
        [
            pa.field("id", pa.int64()),
            pa.field("text", pa.utf8()),
            pa.field("vector", pa.list_(pa.float32(), list_size=2)),
            lancedb.blob("blob"),
        ]
    )
    table = await db.create_table("hybrid_typed_blob", schema=schema)
    await table.add(
        [
            {
                "id": 1,
                "text": "hello alpha",
                "vector": [1.0, 0.0],
                "blob": b"alpha",
            },
            {
                "id": 2,
                "text": "hello beta",
                "vector": [0.9, 0.1],
                "blob": b"beta",
            },
        ]
    )
    await table.create_index("text", config=FTS(with_position=False))

    hits = await (
        table.query()
        .nearest_to([1.0, 0.0])
        .nearest_to_text("hello")
        .select({"blob_alias": col("blob")})
        .limit(2)
        .to_arrow()
    )

    assert "_lance_row_id" in hits.schema.field("blob_alias").type.names
    blobs = await table.fetch_blobs("blob", hits)
    assert {blobs[i].as_py() for i in range(len(blobs))} == {b"alpha", b"beta"}


def test_blob_file_seek_read_and_read_range():
    payload = _identifiable_payload(1024)
    table = _blob_table("seek_read", [{"id": 1, "image": payload}])
    by_id = _row_ids_by_id(table)
    handle = table.fetch_blob_files("image", [by_id[1]])[0]

    assert handle.seek(100) == 100
    assert handle.read(16) == payload[100:116]
    handle.seek(100)
    assert handle.read_range(500, 8) == payload[500:508]
    assert handle.tell() == 100

    with pytest.raises(ValueError, match="whence"):
        handle.seek(0, 99)


def test_blob_file_matches_file_error_and_range_contract():
    payload = b"abcdef"
    table = _blob_table("file_contract", [{"id": 1, "image": payload}])
    row_id = _row_ids_by_id(table)[1]
    handle = table.fetch_blob_files("image", [row_id])[0]

    handle.seek(2)
    for offset, whence in [(-1, io.SEEK_SET), (-3, io.SEEK_CUR), (-7, io.SEEK_END)]:
        with pytest.raises(ValueError, match="negative seek value -1"):
            handle.seek(offset, whence)
        assert handle.tell() == 2

    assert handle.read(None) == payload[2:]
    handle.seek(2)
    assert handle.read1(2) == payload[2:4]
    assert handle.read1() == payload[4:]
    handle.seek(2)
    assert handle.read_ranges([(0, 2), (4, 2), (2, 0)]) == [b"ab", b"ef", b""]
    assert handle.tell() == 2

    with pytest.raises(ValueError):
        handle.read_range(5, 2)
    with pytest.raises(ValueError):
        handle.read_ranges([(0, 1), (5, 2)])
    assert handle.tell() == 2

    handle.close()
    with pytest.raises(ValueError, match="read of closed file"):
        handle.read(1)
    with pytest.raises(ValueError, match="read of closed file"):
        handle.read(None)
    with pytest.raises(ValueError, match="read of closed file"):
        handle.read1(1)
    with pytest.raises(ValueError, match="read of closed file"):
        handle.readall()
    with pytest.raises(ValueError, match="readinto of closed file"):
        handle.readinto(bytearray(1))
    with pytest.raises(ValueError):
        handle.read_range(0, 1)
    with pytest.raises(ValueError):
        handle.read_ranges([])


def test_fetch_blob_files_from_query_partial_read():
    payload = _identifiable_payload(65536)
    table = _blob_table("query_partial", [{"id": 1, "image": payload}])
    hits = table.search().select(["id", "image"]).limit(1).to_arrow()
    assert "_rowid" not in hits.column_names

    handle = table.fetch_blob_files("image", hits)[0]
    assert handle.size() == 65536
    assert handle.read_range(0, 128) == payload[:128]
    assert handle.tell() == 0
    assert handle.seek(40000) == 40000
    assert handle.read(16) == payload[40000:40016]


def test_blob_file_buffered_reader():
    payload = _identifiable_payload(4096)
    table = _blob_table("buffered_reader", [{"id": 1, "image": payload}])
    hits = table.search().select(["id", "image"]).limit(1).to_arrow()
    handle = table.fetch_blob_files("image", hits)[0]
    reader = io.BufferedReader(handle)
    assert reader.read(8) == payload[:8]
    assert reader.read(8) == payload[8:16]
    assert reader.read() == payload[16:]


def test_fetch_blob_files_cross_fragment_nulls_and_dups():
    db = lancedb.connect("memory:///")
    schema = pa.schema([pa.field("id", pa.int64()), lancedb.blob("image")])
    table = db.create_table("cross_fragment", schema=schema)
    table.add([{"id": 1, "image": b"alpha"}])
    table.add([{"id": 2, "image": None}, {"id": 3, "image": b"beta"}])

    by_id = _row_ids_by_id(table)
    request = [by_id[3], by_id[2], by_id[1], by_id[3]]
    handles = table.fetch_blob_files("image", request)
    assert len(handles) == 4
    assert handles[1] is None
    assert handles[0].read() == b"beta"
    assert handles[2].read() == b"alpha"
    assert handles[3].seek(1) == 1
    assert handles[3].read() == b"eta"


def test_blob_file_pyav_decode_seek(tmp_path):
    av = pytest.importorskip("av")
    import fractions

    clip = tmp_path / "clip.mp4"
    with av.open(str(clip), mode="w") as container:
        stream = container.add_stream("mpeg4", rate=5)
        stream.width, stream.height, stream.pix_fmt = 32, 32, "yuv420p"
        stream.time_base = fractions.Fraction(1, 5)
        for pts in range(5):
            frame = av.VideoFrame(32, 32, "yuv420p")
            frame.pts = pts
            container.mux(stream.encode(frame))
        container.mux(stream.encode(None))

    table = _blob_table("pyav", [{"id": 1, "image": clip.read_bytes()}])
    hits = table.search().select(["image"]).limit(1).to_arrow()
    handle = table.fetch_blob_files("image", hits)[0]

    with av.open(handle) as container:
        stream = container.streams.video[0]
        container.seek(0)
        assert next(container.decode(stream)) is not None


def test_blob_v2_hybrid_fetch_blob_files_seek():
    table = _hybrid_blob_table(lancedb.connect("memory:///"))
    hits = (
        table.search(query_type="hybrid")
        .vector([1.0, 0.0])
        .text("hello")
        .select(["id", "image"])
        .limit(2)
        .to_arrow()
    )
    assert "_rowid" not in hits.column_names

    handles = table.fetch_blob_files("image", hits)
    assert len(handles) == 2
    assert {handle.read_range(0, 2) for handle in handles} == {b"al", b"be"}
    first = handles[0]
    assert first.seek(1) == 1
    assert first.read(2) in {b"lp", b"et"}


def test_blob_file_header_sniff_from_search():
    payload = b"%PDF-1.7\n" + bytes(4096)
    table = _blob_table("header_sniff", [{"id": 1, "image": payload}])
    hits = table.search().select(["id", "image"]).limit(1).to_arrow()
    handle = table.fetch_blob_files("image", hits)[0]
    assert handle.read_range(0, 4) == b"%PDF"
    assert handle.tell() == 0


def test_blob_file_multiple_handles_independent_cursors():
    table = _blob_table(
        "multi_handle",
        [{"id": 1, "image": b"first-payload"}, {"id": 2, "image": b"second-payload"}],
    )
    by_id = _row_ids_by_id(table)
    first, second = table.fetch_blob_files("image", [by_id[1], by_id[2]])
    assert first.seek(6) == 6
    assert second.tell() == 0
    assert first.read(7) == b"payload"
    assert second.read(6) == b"second"


def test_fetch_blob_files_nested_path_seek():
    db = lancedb.connect("memory:///")
    info = pa.StructArray.from_arrays(
        [
            pa.array(["first", "second"], type=pa.string()),
            _blob_array("blob", [b"nested-alpha", b"nested-beta"]),
        ],
        names=["name", "blob"],
    )
    data = pa.Table.from_arrays(
        [pa.array([1, 2], type=pa.int64()), info],
        names=["id", "info"],
    )
    table = db.create_table("nested_seek", data=data)
    by_id = _row_ids_by_id(table)
    handle = table.fetch_blob_files("info.blob", [by_id[2]])[0]
    assert handle.seek(7) == 7
    assert handle.read() == b"beta"


def test_fetch_blobs_survives_sort_after_query():
    table = _blob_table(
        "sort_survives",
        [{"id": i, "image": f"payload-{i}".encode()} for i in range(5)],
    )
    hits = table.search().select(["id", "image"]).to_arrow()
    sort_idx = pc.sort_indices(hits["id"], sort_keys=[("id", "descending")])
    sorted_hits = hits.take(sort_idx)

    blobs = table.fetch_blobs("image", sorted_hits)
    expected = [f"payload-{i}".encode() for i in sorted_hits["id"].to_pylist()]
    assert [blobs[i].as_py() for i in range(len(blobs))] == expected


def test_fetch_blobs_survives_filter_and_sort_after_query():
    table = _blob_table(
        "filter_sort_survives",
        [{"id": i, "image": f"payload-{i}".encode()} for i in range(5)],
    )
    hits = table.search().select(["id", "image"]).to_arrow()
    filtered = hits.filter(pc.field("id") >= 2)
    sort_idx = pc.sort_indices(filtered["id"], sort_keys=[("id", "descending")])
    filtered_sorted = filtered.take(sort_idx)

    blobs = table.fetch_blobs("image", filtered_sorted)
    expected = [f"payload-{i}".encode() for i in filtered_sorted["id"].to_pylist()]
    assert [blobs[i].as_py() for i in range(len(blobs))] == expected


def test_fetch_blob_files_survives_sort_after_query():
    table = _blob_table(
        "lazy_sort_survives",
        [{"id": i, "image": f"payload-{i}".encode()} for i in range(5)],
    )
    hits = table.search().select(["id", "image"]).to_arrow()
    sort_idx = pc.sort_indices(hits["id"], sort_keys=[("id", "descending")])
    sorted_hits = hits.take(sort_idx)

    handles = table.fetch_blob_files("image", sorted_hits)
    expected = [f"payload-{i}".encode() for i in sorted_hits["id"].to_pylist()]
    assert [handle.read() for handle in handles] == expected


def test_fetch_blobs_nested_path_survives_sort_after_query():
    db = lancedb.connect("memory:///")
    values = [f"payload-{i}".encode() for i in range(4)]
    info = pa.StructArray.from_arrays(
        [pa.array(["row"] * 4, type=pa.string()), _blob_array("blob", values)],
        names=["name", "blob"],
    )
    data = pa.Table.from_arrays(
        [pa.array(range(4), type=pa.int64()), info],
        names=["id", "info"],
    )
    table = db.create_table("nested_sort_survives", data=data)

    hits = table.search().to_arrow()
    sort_idx = pc.sort_indices(hits["id"], sort_keys=[("id", "descending")])
    sorted_hits = hits.take(sort_idx)

    blobs = table.fetch_blobs("info.blob", sorted_hits)
    expected = [f"payload-{i}".encode() for i in sorted_hits["id"].to_pylist()]
    assert [blobs[i].as_py() for i in range(len(blobs))] == expected


def _identifiable_payload(size: int) -> bytes:
    block = 256
    return b"".join(bytes([i % 256]) * block for i in range(size // block))


def _external_uri_blob_array(uris):
    blob_type = lancedb.blob("image").type
    storage_type = blob_type.storage_type
    child_names = [field.name for field in storage_type]
    assert "uri" in child_names, "blob layout no longer has a uri child"
    children = [
        pa.array(uris if field.name == "uri" else [None] * len(uris), type=field.type)
        for field in storage_type
    ]
    storage = pa.StructArray.from_arrays(children, fields=list(storage_type))
    return pa.ExtensionArray.from_storage(blob_type, storage)


def _external_uri_table_and_rows(name, uris):
    db = lancedb.connect("memory:///")
    schema = pa.schema([pa.field("id", pa.int64()), lancedb.blob("image")])
    table = db.create_table(name, schema=schema)
    rows = pa.Table.from_arrays(
        [
            pa.array(range(len(uris)), type=pa.int64()),
            _external_uri_blob_array(uris),
        ],
        schema=schema,
    )
    return table, rows


def test_add_external_uri_struct_round_trips_with_flag(tmp_path):
    payload = b"external-uri-bytes"
    blob_path = tmp_path / "payload.bin"
    blob_path.write_bytes(payload)

    table, rows = _external_uri_table_and_rows("external_struct", [blob_path.as_uri()])
    table.add(rows, allow_external_blob_outside_bases=True)

    hits = table.search().to_arrow()
    blobs = table.fetch_blobs("image", hits)
    assert blobs[0].as_py() == payload


def test_add_external_uri_without_flag_raises(tmp_path):
    blob_path = tmp_path / "payload.bin"
    blob_path.write_bytes(b"unreachable")

    table, rows = _external_uri_table_and_rows("external_no_flag", [blob_path.as_uri()])
    with pytest.raises(ValueError, match="allow_external_blob_outside_bases"):
        table.add(rows)
    assert table.count_rows() == 0


def test_add_external_uri_string_round_trips_with_flag(tmp_path):
    payload = b"external-uri-bytes"
    blob_path = tmp_path / "payload.bin"
    blob_path.write_bytes(payload)

    db = lancedb.connect("memory:///")
    schema = pa.schema([pa.field("id", pa.int64()), lancedb.blob("image")])
    table = db.create_table("external_string", schema=schema)
    table.add(
        [{"id": 1, "image": blob_path.as_uri()}],
        allow_external_blob_outside_bases=True,
    )

    hits = table.search().to_arrow()
    blobs = table.fetch_blobs("image", hits)
    assert blobs[0].as_py() == payload


@pytest.mark.parametrize("as_pandas", [False, True])
@pytest.mark.parametrize("as_blob", [False, True])
@pytest.mark.parametrize("minimal_schema", [False, True])
@pytest.mark.parametrize("position", [0, 4])
def test_add_external_blob_range_matches_storage_schema(
    tmp_path, as_pandas, as_blob, minimal_schema, position
):
    payload = b"0123456789abcdef"
    blob_path = tmp_path / "payload.bin"
    blob_path.write_bytes(payload)
    field = (
        pa.field("image", pa.large_binary(), metadata={"lance-encoding:blob": "true"})
        if minimal_schema
        else lancedb.blob("image")
    )
    db = lancedb.connect(tmp_path / "db")
    table = db.create_table(
        "ranges", schema=pa.schema([pa.field("id", pa.int64()), field])
    )
    value = (
        Blob.from_uri(blob_path.as_uri(), position=position, size=6)
        if as_blob
        else {"uri": blob_path.as_uri(), "position": position, "size": 6}
    )
    rows = [{"id": 1, "image": b"inline"}, {"id": 2, "image": value}]
    data = pd.DataFrame(rows) if as_pandas else rows

    if minimal_schema:
        with pytest.raises(ValueError, match="Blob column 'image'.*storage schema"):
            table.add(data, allow_external_blob_outside_bases=True)
        assert table.count_rows() == 0
    else:
        table.add(data, allow_external_blob_outside_bases=True)
        by_id = _row_ids_by_id(table)
        assert table.fetch_blobs("image", [by_id[1], by_id[2]]).to_pylist() == [
            b"inline",
            payload[position : position + 6],
        ]


@pytest.mark.parametrize("as_pandas", [False, True])
def test_add_minimal_blob_schema_preserves_whole_file_and_inline_values(
    tmp_path, as_pandas
):
    payload = b"whole-file"
    blob_path = tmp_path / "payload.bin"
    blob_path.write_bytes(payload)
    uri = blob_path.as_uri()
    schema = pa.schema(
        [
            pa.field("id", pa.int64()),
            pa.field(
                "image", pa.large_binary(), metadata={"lance-encoding:blob": "true"}
            ),
        ]
    )
    table = lancedb.connect(tmp_path / "db").create_table("minimal", schema=schema)
    values = [
        None,
        b"inline",
        Blob.from_bytes(b"from-blob"),
        uri,
        Blob.from_uri(uri),
        {"uri": uri, "position": None, "size": None},
    ]
    rows = [{"id": i, "image": value} for i, value in enumerate(values)]
    table.add(
        pd.DataFrame(rows) if as_pandas else rows,
        allow_external_blob_outside_bases=True,
    )

    by_id = _row_ids_by_id(table)
    assert table.fetch_blobs(
        "image", [by_id[i] for i in range(len(values))]
    ).to_pylist() == [
        None,
        b"inline",
        b"from-blob",
        payload,
        payload,
        payload,
    ]


@pytest.mark.parametrize("as_pandas", [False, True])
def test_add_bytes_and_uri_in_one_batch_preserves_external_uri(tmp_path, as_pandas):
    payload = b"external-payload"
    blob_path = tmp_path / "payload.bin"
    blob_path.write_bytes(payload)
    rows = [
        {"id": 1, "image": b"inline"},
        {"id": 2, "image": blob_path.as_uri()},
    ]
    db = lancedb.connect(tmp_path / "db")
    schema = pa.schema([pa.field("id", pa.int64()), lancedb.blob("image")])
    table = db.create_table("mixed", schema=schema)

    table.add(
        pd.DataFrame(rows) if as_pandas else rows,
        allow_external_blob_outside_bases=True,
    )

    by_id = _row_ids_by_id(table)
    assert table.fetch_blobs("image", [by_id[1], by_id[2]]).to_pylist() == [
        b"inline",
        payload,
    ]


@pytest.mark.parametrize("as_pandas", [False, True])
def test_add_mixed_python_blob_values_preserves_external_uris(tmp_path, as_pandas):
    payload = b"external-payload"
    blob_path = tmp_path / "payload.bin"
    blob_path.write_bytes(payload)
    uri = blob_path.as_uri()
    rows = [
        {"id": 1, "image": b"inline"},
        {"id": 2, "image": uri},
        {"id": 3, "image": {"data": b"from-dict"}},
        {"id": 4, "image": Blob.from_bytes(b"from-blob")},
        {"id": 5, "image": Blob.from_uri(uri)},
        {"id": 6, "image": {"uri": uri}},
        {"id": 7, "image": None},
    ]
    db = lancedb.connect(tmp_path / "db")
    schema = pa.schema([pa.field("id", pa.int64()), lancedb.blob("image")])
    table = db.create_table("mixed", schema=schema)

    table.add(
        pd.DataFrame(rows) if as_pandas else rows,
        allow_external_blob_outside_bases=True,
    )

    by_id = _row_ids_by_id(table)
    assert table.fetch_blobs("image", [by_id[i] for i in range(1, 8)]).to_pylist() == [
        b"inline",
        payload,
        b"from-dict",
        b"from-blob",
        payload,
        payload,
        None,
    ]


@pytest.mark.parametrize("as_pandas", [False, True])
def test_merge_insert_mixed_python_blobs_validates_external_uri(tmp_path, as_pandas):
    blob_path = tmp_path / "payload.bin"
    blob_path.write_bytes(b"external-payload")
    table = _blob_table("merge_mixed_uri", [{"id": 1, "image": b"before"}])
    rows = [
        {"id": 1, "image": b"updated"},
        {"id": 2, "image": blob_path.as_uri()},
    ]

    with pytest.raises(ValueError, match="outside registered external bases"):
        (
            table.merge_insert("id")
            .when_matched_update_all()
            .when_not_matched_insert_all()
            .execute(pd.DataFrame(rows) if as_pandas else rows)
        )
    assert table.count_rows() == 1


@pytest.mark.parametrize("as_pandas", [False, True])
def test_merge_insert_python_blob_dicts_and_objects(as_pandas):
    table = _blob_table("merge_python_blobs", [{"id": 1, "image": b"before"}])
    rows = [
        {"id": 1, "image": b"updated"},
        {"id": 2, "image": {"data": b"from-dict"}},
        {"id": 3, "image": Blob.from_bytes(b"from-blob")},
    ]

    result = (
        table.merge_insert("id")
        .when_matched_update_all()
        .when_not_matched_insert_all()
        .execute(pd.DataFrame(rows) if as_pandas else rows)
    )

    assert result.num_updated_rows == 1
    assert result.num_inserted_rows == 2
    by_id = _row_ids_by_id(table)
    assert table.fetch_blobs("image", [by_id[i] for i in range(1, 4)]).to_pylist() == [
        b"updated",
        b"from-dict",
        b"from-blob",
    ]


def test_create_table_with_python_blob_dict_and_object():
    db = lancedb.connect("memory:///")
    schema = pa.schema([pa.field("id", pa.int64()), lancedb.blob("image")])
    table = db.create_table(
        "create_python_blobs",
        data=[
            {"id": 1, "image": {"data": b"from-dict"}},
            {"id": 2, "image": Blob.from_bytes(b"from-blob")},
        ],
        schema=schema,
    )

    by_id = _row_ids_by_id(table)
    assert table.fetch_blobs("image", [by_id[1], by_id[2]]).to_pylist() == [
        b"from-dict",
        b"from-blob",
    ]


def _openable_descriptors(table, *, by_address):
    hits = (
        table.search()
        .select(["id", "image", "_rowaddr"])
        .with_row_id(True)
        .limit(100)
        .to_arrow(blob_mode="descriptions")
    )
    version = table.version
    committed_at = next(
        item["timestamp"]
        for item in table.list_versions()
        if item["version"] == version
    )
    descriptors = {}
    for row in hits.to_pylist():
        descriptor = row["image"]
        if descriptor is not None and descriptor.get("kind") is not None:
            descriptor = dict(descriptor)
            descriptor["source"] = {
                "uri": table.to_lance().uri,
                "version": version,
                "committed_at_micros": round(committed_at.timestamp() * 1_000_000),
                "branch": None,
                "column": "image",
                "row_reference": row["_rowaddr" if by_address else "_rowid"],
                "row_is_address": by_address,
            }
        else:
            descriptor = None
        descriptors[row["id"]] = descriptor
    return descriptors


@pytest.mark.parametrize("by_address", [False, True])
def test_open_blob_files_preserve_snapshot_order_nulls_and_empty_values(
    tmp_path, by_address
):
    db = lancedb.connect(str(tmp_path))
    schema = pa.schema([pa.field("id", pa.int64()), lancedb.blob("image")])
    table = db.create_table("videos", schema=schema)
    table.add(
        [
            {"id": 1, "image": b"alpha"},
            {"id": 2, "image": None},
            {"id": 3, "image": b"beta"},
            {"id": 4, "image": b""},
        ]
    )
    descriptors = _openable_descriptors(table, by_address=by_address)
    table.add([{"id": 1, "image": b"replacement"}], mode="overwrite")
    latest_version = table.version
    values = pa.array(
        [descriptors[3], descriptors[2], descriptors[1], descriptors[3], descriptors[4]]
    )
    files = table.open_blob_files(
        pa.chunked_array([values.slice(0, 2), values.slice(2)])
    )
    assert iter(files) is files
    actual = []
    for file in files:
        if file is None:
            actual.append(None)
        else:
            with file:
                actual.append(file.read())
            assert file.closed
    assert actual == [b"beta", None, b"alpha", b"beta", b""]
    with table.open_blob_file(descriptors[1]) as file:
        file.seek(2)
        assert file.read_range(0, 2) == b"al"
        assert file.tell() == 2
        assert file.read(2) == b"ph"
    assert table.version == latest_version
    assert table.open_blob_file(pa.scalar(None, type=values.type)) is None
    other = db.create_table("other", schema=schema)
    with pytest.raises(ValueError, match="different table"):
        other.open_blob_file(descriptors[1])
    stale = dict(descriptors[1], source=dict(descriptors[1]["source"]))
    stale["source"]["committed_at_micros"] += 1
    with pytest.raises(ValueError, match="snapshot has been replaced"):
        table.open_blob_file(stale)
    raw = {key: value for key, value in descriptors[1].items() if key != "source"}
    with pytest.raises(ValueError, match="source"):
        table.open_blob_file(raw)
    with pytest.raises(TypeError, match="descriptor"):
        table.open_blob_file("not a descriptor")


def test_open_blob_files_does_not_open_ahead(monkeypatch):
    table = _blob_table("lazy_descriptor_iteration", [{"id": 1, "image": b"x"}])
    opened = []

    def open_file(descriptor):
        opened.append(descriptor)
        return descriptor

    monkeypatch.setattr(table, "open_blob_file", open_file)
    files = table.open_blob_files(iter([1, None, 2]))
    assert opened == []
    assert next(files) == 1
    assert opened == [1]
    assert list(files) == [None, 2]
    assert opened == [1, None, 2]


@pytest.mark.asyncio
async def test_open_blob_files_async_iterates_snapshot_handles(tmp_path):
    db = lancedb.connect(str(tmp_path))
    table = db.create_table(
        "videos",
        schema=pa.schema([pa.field("id", pa.int64()), lancedb.blob("image")]),
    )
    table.add([{"id": 1, "image": b"alpha"}, {"id": 2, "image": None}])
    descriptors = _openable_descriptors(table, by_address=True)
    async_db = await lancedb.connect_async(str(tmp_path))
    videos = await async_db.open_table("videos")
    file = await videos.open_blob_file(pa.scalar(descriptors[1]))
    with file:
        assert file.read(2) == b"al"
    actual = []
    async for file in videos.open_blob_files(
        [descriptors[2], descriptors[1], descriptors[1]]
    ):
        if file is None:
            actual.append(None)
        else:
            with file:
                actual.append(await file.aread())
    assert actual == [None, b"alpha", b"alpha"]
