# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

"""Tests for installing and clearing an LsmWriteSpec via
`Table.set_lsm_write_spec` / `Table.unset_lsm_write_spec`.
"""

from datetime import timedelta

import lancedb
import pyarrow as pa
import pytest
from lancedb._lancedb import LsmWriteSpec

SCHEMA = pa.schema(
    [
        pa.field("id", pa.utf8(), nullable=False),
        pa.field("v", pa.int32(), nullable=False),
    ]
)


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


def test_set_lsm_write_spec_validates(tmp_path):
    _db, table = _make_table(tmp_path)

    # No PK set yet.
    with pytest.raises(Exception, match="primary key"):
        table.set_lsm_write_spec(LsmWriteSpec.bucket("id", 4))

    table.set_unenforced_primary_key("id")

    # Column mismatch.
    with pytest.raises(Exception, match="match"):
        table.set_lsm_write_spec(LsmWriteSpec.bucket("v", 4))

    # Out-of-range num_buckets.
    with pytest.raises(Exception, match="num_buckets"):
        table.set_lsm_write_spec(LsmWriteSpec.bucket("id", 0))
    with pytest.raises(Exception, match="num_buckets"):
        table.set_lsm_write_spec(LsmWriteSpec.bucket("id", 1025))

    # Happy path then mutation rejected.
    table.set_lsm_write_spec(LsmWriteSpec.bucket("id", 4))
    with pytest.raises(Exception, match="mutation"):
        table.set_lsm_write_spec(LsmWriteSpec.bucket("id", 8))


def test_unset_lsm_write_spec(tmp_path):
    _db, table = _make_table(tmp_path)

    # unset is a no-op when no spec is set.
    table.unset_lsm_write_spec()

    # Install a spec, then remove it; afterwards a fresh spec can be set.
    table.set_unenforced_primary_key("id")
    table.set_lsm_write_spec(LsmWriteSpec.bucket("id", 4))
    table.unset_lsm_write_spec()
    # Idempotent.
    table.unset_lsm_write_spec()
    table.set_lsm_write_spec(LsmWriteSpec.bucket("id", 8))


def test_set_unsharded_spec(tmp_path):
    _db, table = _make_table(tmp_path)
    # Lance MemWAL still requires a primary key on the dataset; Unsharded
    # just skips per-row hashing.
    table.set_unenforced_primary_key("id")
    table.set_lsm_write_spec(LsmWriteSpec.unsharded())
    table.unset_lsm_write_spec()


def test_lsm_write_spec_repr():
    s = LsmWriteSpec.bucket("id", 4)
    assert s.spec_type == "bucket"
    assert s.column == "id"
    assert s.num_buckets == 4
    assert s.maintained_indexes == []
    assert "bucket" in repr(s)
    assert "id" in repr(s)
    assert "4" in repr(s)

    u = LsmWriteSpec.unsharded()
    assert u.spec_type == "unsharded"
    assert u.column is None
    assert u.num_buckets is None
    assert "unsharded" in repr(u)


def test_lsm_write_spec_with_maintained_indexes():
    s = LsmWriteSpec.bucket("id", 4).with_maintained_indexes(["idx_a", "idx_b"])
    assert s.maintained_indexes == ["idx_a", "idx_b"]


@pytest.mark.asyncio
async def test_async_set_unset_lsm_write_spec(tmp_path):
    db = await lancedb.connect_async(
        tmp_path, read_consistency_interval=timedelta(seconds=0)
    )
    table = await db.create_table(
        "t",
        pa.RecordBatchReader.from_batches(SCHEMA, [_batch(["seed"], [0])]),
    )

    await table.set_unenforced_primary_key("id")
    await table.set_lsm_write_spec(LsmWriteSpec.bucket("id", 4))
    await table.unset_lsm_write_spec()
    # unset is idempotent.
    await table.unset_lsm_write_spec()


def test_set_identity_spec(tmp_path):
    _db, table = _make_table(tmp_path)
    # Identity sharding still requires an unenforced primary key on the
    # table; it shards by the raw value of the given column.
    table.set_unenforced_primary_key("id")
    table.set_lsm_write_spec(LsmWriteSpec.identity("v"))
    table.unset_lsm_write_spec()


def test_lsm_write_spec_identity_and_writer_config_defaults():
    s = LsmWriteSpec.identity("v")
    assert s.spec_type == "identity"
    assert s.column == "v"
    assert s.num_buckets is None
    assert "identity" in repr(s)

    s = s.with_writer_config_defaults({"durable_write": "false"})
    assert s.writer_config_defaults == {"durable_write": "false"}
    assert "durable_write" in repr(s)
