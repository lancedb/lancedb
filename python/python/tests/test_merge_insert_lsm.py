# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

"""Tests for the MemWAL LSM ``merge_insert`` dispatch."""

from datetime import timedelta

import lancedb
import pyarrow as pa
import pytest
from lancedb._lancedb import LsmWriteSpec

SCHEMA = pa.schema(
    [
        pa.field("id", pa.int64(), nullable=False),
        pa.field("value", pa.int64(), nullable=False),
    ]
)

REGION_SCHEMA = pa.schema(
    [
        pa.field("id", pa.int64(), nullable=False),
        pa.field("region", pa.utf8(), nullable=False),
    ]
)


def _reader(ids):
    batch = pa.RecordBatch.from_arrays(
        [
            pa.array(ids, type=pa.int64()),
            pa.array(list(range(len(ids))), type=pa.int64()),
        ],
        schema=SCHEMA,
    )
    return pa.RecordBatchReader.from_batches(SCHEMA, [batch])


def _region_reader(rows):
    batch = pa.RecordBatch.from_arrays(
        [
            pa.array([row[0] for row in rows], type=pa.int64()),
            pa.array([row[1] for row in rows], type=pa.utf8()),
        ],
        schema=REGION_SCHEMA,
    )
    return pa.RecordBatchReader.from_batches(REGION_SCHEMA, [batch])


def _bucket_table(tmp_path):
    """A table with ``id`` as the primary key and a single-bucket LSM spec."""
    db = lancedb.connect(tmp_path, read_consistency_interval=timedelta(seconds=0))
    table = db.create_table("t", _reader([1, 2, 3]))
    table.set_unenforced_primary_key("id")
    # num_buckets = 1: every row routes to the single bucket.
    table.set_lsm_write_spec(LsmWriteSpec.bucket("id", 1))
    return table


def test_lsm_merge_insert_bucket(tmp_path):
    table = _bucket_table(tmp_path)
    # Empty `on` defaults to the primary key.
    result = (
        table.merge_insert([])
        .when_matched_update_all()
        .when_not_matched_insert_all()
        .execute(_reader([3, 4, 5]))
    )
    # LSM path: rows go to the MemWAL, so only num_rows is populated.
    assert result.num_rows == 3
    assert result.version == 0
    assert result.num_inserted_rows == 0
    assert result.num_updated_rows == 0


def test_lsm_merge_insert_unsharded(tmp_path):
    db = lancedb.connect(tmp_path, read_consistency_interval=timedelta(seconds=0))
    table = db.create_table("t", _reader([1, 2, 3]))
    table.set_unenforced_primary_key("id")
    table.set_lsm_write_spec(LsmWriteSpec.unsharded())
    result = (
        table.merge_insert("id")
        .when_matched_update_all()
        .when_not_matched_insert_all()
        .execute(_reader([10, 11, 12, 13]))
    )
    assert result.num_rows == 4


def test_lsm_merge_insert_identity(tmp_path):
    db = lancedb.connect(tmp_path, read_consistency_interval=timedelta(seconds=0))
    table = db.create_table("t", _region_reader([(1, "us"), (2, "us")]))
    table.set_unenforced_primary_key("id")
    table.set_lsm_write_spec(LsmWriteSpec.identity("region"))
    # All rows share one identity value, so they route to one shard.
    result = (
        table.merge_insert([])
        .when_matched_update_all()
        .when_not_matched_insert_all()
        .execute(_region_reader([(3, "us"), (4, "us")]))
    )
    assert result.num_rows == 2


def test_lsm_merge_insert_disable_lsm(tmp_path):
    table = _bucket_table(tmp_path)  # rows id = 1, 2, 3
    # disable_lsm() opts out: the standard path runs and commits even with a spec.
    result = (
        table.merge_insert("id")
        .when_not_matched_insert_all()
        .disable_lsm()
        .execute(_reader([3, 4, 5]))
    )
    assert result.num_inserted_rows == 2
    assert table.count_rows() == 5


def test_lsm_merge_insert_validate_single_shard_off(tmp_path):
    table = _bucket_table(tmp_path)
    result = (
        table.merge_insert([])
        .when_matched_update_all()
        .when_not_matched_insert_all()
        .validate_single_shard(False)
        .execute(_reader([6, 7, 8]))
    )
    assert result.num_rows == 3


def test_lsm_merge_insert_no_spec_uses_standard_path(tmp_path):
    # A table with a primary key but no LSM write spec installed.
    db = lancedb.connect(tmp_path, read_consistency_interval=timedelta(seconds=0))
    table = db.create_table("t", _reader([1, 2, 3]))
    table.set_unenforced_primary_key("id")
    # With no spec, a default merge_insert uses the standard path and commits.
    result = (
        table.merge_insert("id")
        .when_matched_update_all()
        .when_not_matched_insert_all()
        .execute(_reader([3, 4, 5]))
    )
    assert result.num_inserted_rows == 2
    assert table.count_rows() == 5


def test_lsm_merge_insert_rejects_on_not_primary_key(tmp_path):
    table = _bucket_table(tmp_path)
    with pytest.raises(Exception, match="primary key"):
        (
            table.merge_insert("value")
            .when_matched_update_all()
            .when_not_matched_insert_all()
            .execute(_reader([1]))
        )


def test_lsm_merge_insert_rejects_non_upsert(tmp_path):
    table = _bucket_table(tmp_path)
    # Insert-only (no when_matched_update_all) is not the upsert shape.
    with pytest.raises(Exception, match="upsert"):
        table.merge_insert([]).when_not_matched_insert_all().execute(_reader([4]))


def test_lsm_close_writers(tmp_path):
    table = _bucket_table(tmp_path)
    (
        table.merge_insert([])
        .when_matched_update_all()
        .when_not_matched_insert_all()
        .execute(_reader([7, 8]))
    )
    table.close_lsm_writers()
    # The writer reopens lazily on the next merge_insert.
    result = (
        table.merge_insert([])
        .when_matched_update_all()
        .when_not_matched_insert_all()
        .execute(_reader([9]))
    )
    assert result.num_rows == 1


@pytest.mark.asyncio
async def test_async_lsm_merge_insert(tmp_path):
    db = await lancedb.connect_async(
        tmp_path, read_consistency_interval=timedelta(seconds=0)
    )
    table = await db.create_table("t", _reader([1, 2, 3]))
    await table.set_unenforced_primary_key("id")
    await table.set_lsm_write_spec(LsmWriteSpec.bucket("id", 1))

    builder = (
        table.merge_insert([]).when_matched_update_all().when_not_matched_insert_all()
    )
    result = await builder.execute(_reader([3, 4, 5]))
    assert result.num_rows == 3
    await table.close_lsm_writers()


def _lsm_upsert(table, ids):
    """Upsert ``ids`` (value = 0..n) through the LSM merge_insert path."""
    (
        table.merge_insert([])
        .when_matched_update_all()
        .when_not_matched_insert_all()
        .execute(_reader(ids))
    )


def test_lsm_read_sees_active_memtable(tmp_path):
    db = lancedb.connect(tmp_path, read_consistency_interval=timedelta(seconds=0))
    table = db.create_table("t", _reader([1, 2, 3]))  # base ids 1,2,3
    table.set_unenforced_primary_key("id")
    table.set_lsm_write_spec(LsmWriteSpec.unsharded())

    _lsm_upsert(table, [4, 5])  # active memtable only, not committed to base

    # Default read auto-routes through the LSM scanner: base ∪ active memtable.
    lsm = table.search().to_arrow()
    assert sorted(lsm["id"].to_pylist()) == [1, 2, 3, 4, 5]

    # disable_lsm() bypasses the MemWAL and reads the base table only.
    base_only = table.search().disable_lsm().to_arrow()
    assert sorted(base_only["id"].to_pylist()) == [1, 2, 3]


def test_lsm_read_dedup_newest_wins(tmp_path):
    db = lancedb.connect(tmp_path, read_consistency_interval=timedelta(seconds=0))
    table = db.create_table("t", _reader([1, 2, 3]))  # id 2 -> value 1
    table.set_unenforced_primary_key("id")
    table.set_lsm_write_spec(LsmWriteSpec.unsharded())

    _lsm_upsert(table, [2, 3, 4])  # ids 2,3,4 -> values 0,1,2

    lsm = table.search().to_arrow().sort_by("id")
    assert lsm["id"].to_pylist() == [1, 2, 3, 4]
    # id 1 from base (value 0); 2,3,4 from memtable (values 0,1,2).
    assert lsm["value"].to_pylist() == [0, 0, 1, 2]


def test_lsm_read_without_spec_reads_base(tmp_path):
    db = lancedb.connect(tmp_path, read_consistency_interval=timedelta(seconds=0))
    table = db.create_table("t", _reader([1, 2, 3]))
    table.set_unenforced_primary_key("id")  # no LSM write spec

    # No spec: default read and disable_lsm both read the base table, no error.
    assert sorted(table.search().to_arrow()["id"].to_pylist()) == [1, 2, 3]
    assert sorted(table.search().disable_lsm().to_arrow()["id"].to_pylist()) == [
        1,
        2,
        3,
    ]


def test_lsm_read_unsupported_shape_errors_without_disable_lsm(tmp_path):
    db = lancedb.connect(tmp_path, read_consistency_interval=timedelta(seconds=0))
    table = db.create_table("t", _reader([1, 2, 3]))
    table.set_unenforced_primary_key("id")
    table.set_lsm_write_spec(LsmWriteSpec.unsharded())
    _lsm_upsert(table, [4])

    # with_row_id is unsupported by the LSM scanner; on a MemWAL table the default
    # (auto-routed) read hard-errors instead of silently reading a stale base.
    with pytest.raises(Exception):
        table.search().with_row_id(True).to_arrow()

    # disable_lsm() is the escape hatch: it reads the base table only.
    base = table.search().with_row_id(True).disable_lsm().to_arrow()
    assert sorted(base["id"].to_pylist()) == [1, 2, 3]


@pytest.mark.asyncio
async def test_async_lsm_read(tmp_path):
    db = await lancedb.connect_async(
        tmp_path, read_consistency_interval=timedelta(seconds=0)
    )
    table = await db.create_table("t", _reader([1, 2, 3]))
    await table.set_unenforced_primary_key("id")
    await table.set_lsm_write_spec(LsmWriteSpec.unsharded())

    builder = (
        table.merge_insert([]).when_matched_update_all().when_not_matched_insert_all()
    )
    await builder.execute(_reader([4, 5]))

    arrow = await table.query().to_arrow()
    assert sorted(arrow["id"].to_pylist()) == [1, 2, 3, 4, 5]
