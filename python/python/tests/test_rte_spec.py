# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

"""Tests for installing and clearing an RTEWriteSpec via
`Table.enable_rte` / `Table.disable_rte`.
"""

from datetime import timedelta

import lancedb
import pyarrow as pa
import pytest
from lancedb._lancedb import RTEWriteSpec
from lancedb.index import BTree

SCHEMA = pa.schema(
    [
        pa.field("id", pa.utf8(), nullable=False),
        pa.field("v", pa.int32(), nullable=False),
    ]
)


def test_rte_spec_module_metadata():
    assert lancedb.RTEWriteSpec is RTEWriteSpec
    assert RTEWriteSpec.__module__ == "lancedb._lancedb"


def _batch(ids, vs):
    return pa.RecordBatch.from_arrays(
        [pa.array(ids, type=pa.utf8()), pa.array(vs, type=pa.int32())],
        schema=SCHEMA,
    )


def _reader(ids, vs):
    return pa.RecordBatchReader.from_batches(SCHEMA, [_batch(ids, vs)])


def _make_table(tmp_path):
    db = lancedb.connect(tmp_path, read_consistency_interval=timedelta(seconds=0))
    table = db.create_table("t", _reader(["seed"], [0]))
    return db, table


def test_enable_rte_validates(tmp_path):
    _db, table = _make_table(tmp_path)
    table.set_unenforced_primary_key("id")

    # Out-of-range num_buckets.
    with pytest.raises(Exception, match="num_buckets"):
        table.enable_rte(RTEWriteSpec.bucket("id", 0))
    with pytest.raises(Exception, match="num_buckets"):
        table.enable_rte(RTEWriteSpec.bucket("id", 1025))

    # Happy path, then a set over the installed spec is refused.
    table.enable_rte(RTEWriteSpec.bucket("id", 4))
    with pytest.raises(Exception, match="already set"):
        table.enable_rte(RTEWriteSpec.bucket("id", 8))


def test_disable_rte(tmp_path):
    _db, table = _make_table(tmp_path)
    table.set_unenforced_primary_key("id")

    # unset errors when no spec is set.
    with pytest.raises(Exception, match="no RTE write spec"):
        table.disable_rte()

    # Install a spec, then remove it; afterwards a fresh spec can be set.
    table.enable_rte(RTEWriteSpec.bucket("id", 4))
    table.disable_rte()
    # A second unset errors — there is no spec left to remove.
    with pytest.raises(Exception, match="no RTE write spec"):
        table.disable_rte()
    table.enable_rte(RTEWriteSpec.bucket("id", 8))


def test_enable_rte_requires_primary_key(tmp_path):
    _db, table = _make_table(tmp_path)
    for spec in (
        RTEWriteSpec.unsharded(),
        RTEWriteSpec.bucket("id", 4),
        RTEWriteSpec.identity("id"),
    ):
        with pytest.raises(Exception, match="primary key"):
            table.enable_rte(spec)
    assert table.get_rte_spec() is None


def test_set_unsharded_spec(tmp_path):
    _db, table = _make_table(tmp_path)
    # Lance MemWAL still requires a primary key on the dataset; Unsharded
    # just skips per-row hashing.
    table.set_unenforced_primary_key("id")
    table.enable_rte(RTEWriteSpec.unsharded())
    table.disable_rte()


def test_rte_spec_repr():
    s = RTEWriteSpec.bucket("id", 4)
    assert s.spec_type == "bucket"
    assert s.column == "id"
    assert s.num_buckets == 4
    # A fresh spec defers its maintained set to install time.
    assert s.maintained_indexes is None
    assert s.with_maintained_indexes([]).maintained_indexes == []
    assert "bucket" in repr(s)
    assert "id" in repr(s)
    assert "4" in repr(s)

    u = RTEWriteSpec.unsharded()
    assert u.spec_type == "unsharded"
    assert u.column is None
    assert u.num_buckets is None
    assert "unsharded" in repr(u)


def test_rte_spec_with_maintained_indexes():
    s = RTEWriteSpec.bucket("id", 4).with_maintained_indexes(["idx_a", "idx_b"])
    assert s.maintained_indexes == ["idx_a", "idx_b"]


@pytest.mark.asyncio
async def test_async_set_disable_rte(tmp_path):
    db = await lancedb.connect_async(
        tmp_path, read_consistency_interval=timedelta(seconds=0)
    )
    table = await db.create_table(
        "t",
        pa.RecordBatchReader.from_batches(SCHEMA, [_batch(["seed"], [0])]),
    )

    await table.set_unenforced_primary_key("id")
    await table.enable_rte(RTEWriteSpec.bucket("id", 4))
    await table.disable_rte()
    # A second unset errors.
    with pytest.raises(Exception, match="no RTE write spec"):
        await table.disable_rte()


def test_set_identity_spec(tmp_path):
    _db, table = _make_table(tmp_path)
    # Identity sharding still requires an unenforced primary key on the
    # table; it shards by the raw value of the given column.
    table.set_unenforced_primary_key("id")
    table.enable_rte(RTEWriteSpec.identity("v"))
    table.disable_rte()


def test_rte_spec_identity_and_writer_config_defaults():
    s = RTEWriteSpec.identity("v")
    assert s.spec_type == "identity"
    assert s.column == "v"
    assert s.num_buckets is None
    assert "identity" in repr(s)

    s = s.with_writer_config_defaults({"durable_write": "false"})
    assert s.writer_config_defaults == {"durable_write": "false"}
    assert "durable_write" in repr(s)


def test_get_rte_spec(tmp_path):
    _db, table = _make_table(tmp_path)
    table.set_unenforced_primary_key("id")

    # None when nothing is installed.
    assert table.get_rte_spec() is None

    # A real scalar index is needed to name it as a maintained index.
    table.create_index("id", config=BTree())
    idx_name = table.list_indices()[0].name

    # Bucket spec round-trips, including maintained indexes and writer config
    # defaults.
    table.enable_rte(
        RTEWriteSpec.bucket("id", 4)
        .with_maintained_indexes([idx_name])
        .with_writer_config_defaults({"durable_write": "false"})
    )
    spec = table.get_rte_spec()
    assert spec is not None
    assert spec.spec_type == "bucket"
    assert spec.column == "id"
    assert spec.num_buckets == 4
    assert spec.maintained_indexes == [idx_name]
    assert spec.writer_config_defaults == {"durable_write": "false"}

    # After unset, None again.
    table.disable_rte()
    assert table.get_rte_spec() is None

    # Identity round-trips (column recovered from the schema). An unnamed
    # maintained set reads back as None: what is stored is the intent to
    # maintain every index, not the set it resolves to now.
    table.enable_rte(RTEWriteSpec.identity("id"))
    spec = table.get_rte_spec()
    assert spec.spec_type == "identity"
    assert spec.column == "id"
    assert spec.maintained_indexes is None
    table.disable_rte()

    # Unsharded round-trips (no routing column). Opting out is distinct from
    # the unnamed default.
    table.enable_rte(RTEWriteSpec.unsharded().with_maintained_indexes([]))
    spec = table.get_rte_spec()
    assert spec.spec_type == "unsharded"
    assert spec.column is None
    assert spec.maintained_indexes == []


@pytest.mark.asyncio
async def test_async_get_rte_spec(tmp_path):
    db = await lancedb.connect_async(
        tmp_path, read_consistency_interval=timedelta(seconds=0)
    )
    table = await db.create_table(
        "t",
        pa.RecordBatchReader.from_batches(SCHEMA, [_batch(["seed"], [0])]),
    )

    assert await table.get_rte_spec() is None
    await table.set_unenforced_primary_key("id")

    # A real scalar index is needed to name it as a maintained index.
    await table.create_index("id", config=BTree())
    idx_name = (await table.list_indices())[0].name

    await table.enable_rte(
        RTEWriteSpec.bucket("id", 8).with_maintained_indexes([idx_name])
    )
    spec = await table.get_rte_spec()
    assert spec is not None
    assert spec.spec_type == "bucket"
    assert spec.column == "id"
    assert spec.num_buckets == 8
    assert spec.maintained_indexes == [idx_name]
    await table.disable_rte()
    assert await table.get_rte_spec() is None
