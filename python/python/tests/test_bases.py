# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

import pyarrow as pa
import pytest

import lancedb


def test_add_bases_accepts_named_and_dataset_root(tmp_path):
    media = tmp_path / "media"
    parent = tmp_path / "parent"
    media.mkdir()
    parent.mkdir()
    db = lancedb.connect(tmp_path / "db")
    schema = pa.schema([pa.field("id", pa.int64())])
    table = db.create_table("photos", schema=schema)
    table.add_bases(
        [
            lancedb.TableBase(path=media.as_uri(), name="media", is_dataset_root=False),
            lancedb.TableBase(
                path=parent.as_uri(), name="parent", is_dataset_root=True
            ),
        ]
    )


def test_add_bases_accepts_two_unnamed_paths(tmp_path):
    media = tmp_path / "media"
    other = tmp_path / "other"
    media.mkdir()
    other.mkdir()
    db = lancedb.connect(tmp_path / "db")
    schema = pa.schema([pa.field("id", pa.int64())])
    table = db.create_table("photos", schema=schema)
    table.add_bases([media.as_uri(), other.as_uri()])


def test_add_bases_rejects_dict_input(tmp_path):
    db = lancedb.connect(tmp_path / "db")
    schema = pa.schema([pa.field("id", pa.int64())])
    table = db.create_table("photos", schema=schema)
    with pytest.raises(TypeError, match="TableBase"):
        table.add_bases({"path": "s3://bucket/media/"})


@pytest.mark.asyncio
async def test_async_add_bases_accepts_file_uri(tmp_path):
    media = tmp_path / "media"
    media.mkdir()
    db = await lancedb.connect_async(tmp_path / "db")
    schema = pa.schema([pa.field("id", pa.int64())])
    table = await db.create_table("photos", schema=schema)
    await table.add_bases(media.as_uri())


def test_memory_add_bases_accepts_file_uri(tmp_path):
    media = tmp_path / "media"
    media.mkdir()
    db = lancedb.connect("memory:///")
    schema = pa.schema([pa.field("id", pa.int64())])
    table = db.create_table("photos", schema=schema)
    table.add_bases(media.as_uri())


def test_namespace_add_bases_accepts_file_uri(tmp_path):
    media = tmp_path / "media"
    media.mkdir()
    db = lancedb.connect_namespace("dir", {"root": str(tmp_path / "ns")})
    schema = pa.schema([pa.field("id", pa.int64())])
    table = db.create_table("photos", schema=schema)
    table.add_bases(media.as_uri())
