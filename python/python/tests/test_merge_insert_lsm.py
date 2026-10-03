# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

"""Tests for the MemWAL LSM ``merge_insert`` dispatch."""

from datetime import timedelta

import lancedb
import pyarrow as pa
import pytest
from lancedb._lancedb import LsmWriteSpec
from lancedb.index import FTS, IvfPq

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


def test_lsm_merge_insert_use_lsm_false(tmp_path):
    table = _bucket_table(tmp_path)  # rows id = 1, 2, 3
    # use_lsm(False) opts out: the standard path runs and commits even with a spec.
    result = (
        table.merge_insert("id")
        .when_not_matched_insert_all()
        .use_lsm(False)
        .execute(_reader([3, 4, 5]))
    )
    assert result.num_inserted_rows == 2
    assert table.count_rows() == 5


def test_lsm_merge_insert_use_lsm_true_without_spec_errors(tmp_path):
    # A table with a primary key but no LSM write spec installed.
    db = lancedb.connect(tmp_path, read_consistency_interval=timedelta(seconds=0))
    table = db.create_table("t", _reader([1, 2, 3]))
    table.set_unenforced_primary_key("id")
    # use_lsm(True) demands MemWAL routing; without a spec it errors.
    with pytest.raises(Exception, match="use_lsm"):
        (
            table.merge_insert("id")
            .when_matched_update_all()
            .when_not_matched_insert_all()
            .use_lsm(True)
            .execute(_reader([3, 4, 5]))
        )


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

    # use_lsm(False) bypasses the MemWAL and reads the base table only.
    base_only = table.search().use_lsm(False).to_arrow()
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

    # No spec: default read and use_lsm(False) both read the base table, no error.
    assert sorted(table.search().to_arrow()["id"].to_pylist()) == [1, 2, 3]
    assert sorted(table.search().use_lsm(False).to_arrow()["id"].to_pylist()) == [
        1,
        2,
        3,
    ]


def test_lsm_read_unsupported_shape_errors_without_use_lsm_false(tmp_path):
    db = lancedb.connect(tmp_path, read_consistency_interval=timedelta(seconds=0))
    table = db.create_table("t", _reader([1, 2, 3]))
    table.set_unenforced_primary_key("id")
    table.set_lsm_write_spec(LsmWriteSpec.unsharded())
    _lsm_upsert(table, [4])

    # with_row_id is unsupported by the LSM scanner; on a MemWAL table the default
    # (auto-routed) read hard-errors instead of silently reading a stale base.
    with pytest.raises(Exception):
        table.search().with_row_id(True).to_arrow()

    # use_lsm(False) is the escape hatch: it reads the base table only.
    base = table.search().with_row_id(True).use_lsm(False).to_arrow()
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


VECTOR_DIM = 8

VECTOR_SCHEMA = pa.schema(
    [
        pa.field("id", pa.int64(), nullable=False),
        pa.field("category", pa.utf8(), nullable=False),
        pa.field("vector", pa.list_(pa.float32(), VECTOR_DIM), nullable=False),
    ]
)


def _vector_reader(rows):
    """Rows are ``(id, category, [f32; VECTOR_DIM])`` tuples."""
    batch = pa.RecordBatch.from_arrays(
        [
            pa.array([row[0] for row in rows], type=pa.int64()),
            pa.array([row[1] for row in rows], type=pa.utf8()),
            pa.array([row[2] for row in rows], type=pa.list_(pa.float32(), VECTOR_DIM)),
        ],
        schema=VECTOR_SCHEMA,
    )
    return pa.RecordBatchReader.from_batches(VECTOR_SCHEMA, [batch])


def _vector_table(tmp_path):
    """Base table whose vector column is indexed so its rows are visible to the LSM
    vector scanner (the base arm uses ``fast_search`` — indexed data only), plus an
    unsharded LSM spec that maintains that index for the memtable.

    Rows 1,2 are category ``a``, row 3 is ``b``, and 4..60 are filler ``c`` that
    give the tiny IVF index enough data to train.
    """
    db = lancedb.connect(tmp_path, read_consistency_interval=timedelta(seconds=0))
    rows = [
        (
            i,
            "a" if i in (1, 2) else "b" if i == 3 else "c",
            [float((i * 7 + j) % 13) for j in range(VECTOR_DIM)],
        )
        for i in range(1, 61)
    ]
    table = db.create_table("t", _vector_reader(rows))
    table.set_unenforced_primary_key("id")
    # num_partitions=1 makes the search exhaustive within the single partition
    # (deterministic); num_bits=4 keeps PQ training viable on a tiny dataset.
    table.create_index(
        "vector", config=IvfPq(num_partitions=1, num_sub_vectors=2, num_bits=4)
    )
    index_name = table.list_indices()[0].name
    table.set_lsm_write_spec(
        LsmWriteSpec.unsharded().with_maintained_indexes([index_name])
    )
    return table


def _vector_upsert(table, rows):
    (
        table.merge_insert([])
        .when_matched_update_all()
        .when_not_matched_insert_all()
        .execute(_vector_reader(rows))
    )


def test_lsm_read_vector_sees_memtable(tmp_path):
    table = _vector_table(tmp_path)
    # id 1000 lands in the active memtable, not committed to the base table.
    _vector_upsert(table, [(1000, "a", [1.0] * VECTOR_DIM)])

    query = [1.0] * VECTOR_DIM
    # Vector search auto-routes through the LSM scanner: indexed base ∪ memtable.
    ids = set(table.search(query).limit(100).to_arrow()["id"].to_pylist())
    assert {1, 2, 3} <= ids  # indexed base rows
    assert 1000 in ids  # in-flight memtable row

    # use_lsm(False) bypasses the MemWAL, so the in-flight row is not visible.
    base_ids = set(
        table.search(query).use_lsm(False).limit(100).to_arrow()["id"].to_pylist()
    )
    assert {1, 2, 3} <= base_ids
    assert 1000 not in base_ids


def test_lsm_read_vector_prefilter(tmp_path):
    table = _vector_table(tmp_path)
    # in-flight rows in both categories.
    _vector_upsert(
        table, [(1000, "a", [1.0] * VECTOR_DIM), (1001, "b", [1.0] * VECTOR_DIM)]
    )

    query = [1.0] * VECTOR_DIM
    # The `where` predicate must apply as a prefilter across base ∪ memtable —
    # regression test for the vector arm silently dropping the filter.
    rows = table.search(query).where("category = 'a'").limit(100).to_arrow()
    assert set(rows["id"].to_pylist()) == {1, 2, 1000}
    assert set(rows["category"].to_pylist()) == {"a"}

    # Sanity: without the filter, other categories are returned too.
    unfiltered = set(table.search(query).limit(100).to_arrow()["category"].to_pylist())
    assert unfiltered != {"a"}


def test_lsm_read_plain_prefilter(tmp_path):
    table = _vector_table(tmp_path)
    _vector_upsert(
        table, [(1000, "a", [1.0] * VECTOR_DIM), (1001, "b", [1.0] * VECTOR_DIM)]
    )

    # Plain scan + filter over base ∪ memtable: base 'a' rows 1,2 and memtable 1000.
    rows = table.search().where("category = 'a'").to_arrow()
    assert set(rows["id"].to_pylist()) == {1, 2, 1000}


FTS_SCHEMA = pa.schema(
    [
        pa.field("id", pa.int64(), nullable=False),
        pa.field("text", pa.utf8(), nullable=False),
    ]
)


def _fts_reader(rows):
    """Rows are ``(id, text)`` tuples."""
    batch = pa.RecordBatch.from_arrays(
        [
            pa.array([row[0] for row in rows], type=pa.int64()),
            pa.array([row[1] for row in rows], type=pa.utf8()),
        ],
        schema=FTS_SCHEMA,
    )
    return pa.RecordBatchReader.from_batches(FTS_SCHEMA, [batch])


def test_lsm_read_fts_sees_memtable(tmp_path):
    db = lancedb.connect(tmp_path, read_consistency_interval=timedelta(seconds=0))
    table = db.create_table(
        "t",
        _fts_reader(
            [
                (1, "the quick brown fox"),
                (2, "lazy dog sleeps"),
                (3, "quick red fox"),
            ]
        ),
    )
    table.set_unenforced_primary_key("id")
    # Native FTS index (tantivy is not compatible with the LSM memtable index).
    table.create_index("text", config=FTS())
    index_name = table.list_indices()[0].name
    table.set_lsm_write_spec(
        LsmWriteSpec.unsharded().with_maintained_indexes([index_name])
    )

    # in-flight doc 4 lands in the memtable's maintained FTS index.
    (
        table.merge_insert([])
        .when_matched_update_all()
        .when_not_matched_insert_all()
        .execute(_fts_reader([(4, "brown fox jumps")]))
    )

    # Full-text search auto-routes through the LSM scanner: base ∪ memtable.
    ids = set(
        table.search("fox", query_type="fts", fts_columns="text")
        .limit(10)
        .to_arrow()["id"]
        .to_pylist()
    )
    assert ids == {1, 3, 4}

    # Prefilter restricts the FTS results across both tiers.
    filtered = set(
        table.search("fox", query_type="fts", fts_columns="text")
        .where("id > 1")
        .limit(10)
        .to_arrow()["id"]
        .to_pylist()
    )
    assert filtered == {3, 4}


def test_lsm_read_vector_unsupported_knobs_error(tmp_path):
    table = _vector_table(tmp_path)
    _vector_upsert(table, [(1000, "a", [1.0] * VECTOR_DIM)])
    query = [1.0] * VECTOR_DIM

    # distance_range and use_index(False) change the vector result set/mode, which
    # the LSM scanner can't honor, so it hard-errors instead of silently returning
    # wrong results (matching the prefilter / unsupported-shape contract).
    with pytest.raises(Exception, match="distance_range"):
        table.search(query).distance_range(0.0, 0.5).to_arrow()
    with pytest.raises(Exception, match="use_index"):
        table.search(query).bypass_vector_index().to_arrow()

    # use_lsm(False) is the escape hatch: the base-only standard path honors them.
    base = table.search(query).distance_range(0.0, 100.0).use_lsm(False).to_arrow()
    assert 1000 not in set(base["id"].to_pylist())


def test_lsm_read_vector_limit_offset(tmp_path):
    table = _vector_table(tmp_path)
    _vector_upsert(table, [(1000, "a", [1.0] * VECTOR_DIM)])
    query = [1.0] * VECTOR_DIM
    # Lance's plan_vector over-fetches k + offset internally, so paging is correct:
    # the second page is a full page (not truncated) and disjoint from the first.
    page1 = table.search(query).limit(3).offset(0).to_arrow()["id"].to_pylist()
    page2 = table.search(query).limit(3).offset(3).to_arrow()["id"].to_pylist()
    assert len(page1) == 3
    # If k ignored offset, page2 would be empty (limit - offset = 0); a full second
    # page that differs from the first proves offset widens the candidate pool.
    assert len(page2) == 3
    assert set(page1) != set(page2)


def test_lsm_read_vector_postfilter_errors(tmp_path):
    table = _vector_table(tmp_path)
    _vector_upsert(table, [(1000, "a", [1.0] * VECTOR_DIM)])
    query = [1.0] * VECTOR_DIM
    # The LSM scanner always prefilters; a requested postfilter changes results, so
    # it hard-errors rather than silently prefiltering.
    with pytest.raises(Exception, match="postfilter"):
        table.search(query).where("category = 'a'").postfilter().to_arrow()


def test_lsm_read_projection_excludes_pk(tmp_path):
    table = _vector_table(tmp_path)
    _vector_upsert(table, [(1000, "a", [1.0] * VECTOR_DIM)])
    # Selecting only 'category' must not leak the 'id' primary key Lance appends
    # internally for dedup.
    rows = table.search().select(["category"]).where("category = 'a'").to_arrow()
    assert rows.column_names == ["category"]


def test_lsm_read_fts_unmaintained_index_errors(tmp_path):
    db = lancedb.connect(tmp_path, read_consistency_interval=timedelta(seconds=0))
    table = db.create_table("t", _fts_reader([(1, "quick fox"), (2, "lazy dog")]))
    table.set_unenforced_primary_key("id")
    table.create_index("text", config=FTS())
    # No maintained indexes: the active memtable FTS arm cannot serve un-compacted
    # docs, so the search would silently omit them — reject instead.
    table.set_lsm_write_spec(LsmWriteSpec.unsharded().with_maintained_indexes([]))
    with pytest.raises(Exception, match="maintained"):
        table.search("fox", query_type="fts", fts_columns="text").to_arrow()


def test_lsm_read_time_travel_errors(tmp_path):
    db = lancedb.connect(tmp_path, read_consistency_interval=timedelta(seconds=0))
    table = db.create_table("t", _reader([1, 2, 3]))
    table.set_unenforced_primary_key("id")
    table.set_lsm_write_spec(LsmWriteSpec.unsharded())
    pinned = table.version
    table.add(_reader([4, 5]))  # standard add commits a newer version
    table.checkout(pinned)  # detached head at the historical version

    # The WAL/manifest expose current live state, so an LSM read at a pinned
    # historical version is rejected.
    with pytest.raises(Exception, match="time-travel"):
        table.search().to_arrow()
    # use_lsm(False) reads the base table at the pinned version.
    base = table.search().use_lsm(False).to_arrow()
    assert sorted(base["id"].to_pylist()) == [1, 2, 3]


def test_lsm_read_take_row_ids_errors(tmp_path):
    db = lancedb.connect(tmp_path, read_consistency_interval=timedelta(seconds=0))
    table = db.create_table("t", _reader([1, 2, 3]))
    table.set_unenforced_primary_key("id")
    table.set_lsm_write_spec(LsmWriteSpec.unsharded())
    _lsm_upsert(table, [4])
    # take-by-row-id auto-routes through the LSM scanner, which has no stable _rowid,
    # so it hard-errors instead of failing with an opaque column-not-found error.
    with pytest.raises(Exception, match="row id"):
        table.take_row_ids([0, 1]).to_arrow()
    # use_lsm(False) is the escape hatch: it reads the base table.
    base = table.take_row_ids([0, 1]).use_lsm(False).to_arrow()
    assert base.num_rows == 2


def test_lsm_read_fts_postfilter_errors(tmp_path):
    db = lancedb.connect(tmp_path, read_consistency_interval=timedelta(seconds=0))
    table = db.create_table("t", _fts_reader([(1, "quick fox"), (2, "lazy dog")]))
    table.set_unenforced_primary_key("id")
    table.create_index("text", config=FTS())
    index_name = table.list_indices()[0].name
    table.set_lsm_write_spec(
        LsmWriteSpec.unsharded().with_maintained_indexes([index_name])
    )
    # The LSM scanner always prefilters; postfilter on FTS changes result semantics,
    # so it hard-errors (previously only the vector arm rejected it).
    with pytest.raises(Exception, match="postfilter"):
        (
            table.search("fox", query_type="fts", fts_columns="text")
            .where("id > 0")
            .postfilter()
            .to_arrow()
        )


def test_lsm_read_fts_multiple_same_type_indexes_errors(tmp_path):
    db = lancedb.connect(tmp_path, read_consistency_interval=timedelta(seconds=0))
    table = db.create_table("t", _fts_reader([(1, "quick fox"), (2, "lazy dog")]))
    table.set_unenforced_primary_key("id")
    table.create_index("text", config=FTS(), name="fts_a")
    table.create_index("text", config=FTS(), name="fts_b", replace=False)
    table.set_lsm_write_spec(
        LsmWriteSpec.unsharded().with_maintained_indexes(["fts_a"])
    )
    # Two FTS indexes on the column: the base planner's chosen index is ambiguous, so
    # the scanner can't pick a catch-up watermark and rejects rather than risk
    # dropping rows the actually-used index has not caught up to.
    with pytest.raises(Exception, match="multiple"):
        table.search("fox", query_type="fts", fts_columns="text").to_arrow()


def test_lsm_read_vector_unmaintained_index_errors(tmp_path):
    db = lancedb.connect(tmp_path, read_consistency_interval=timedelta(seconds=0))
    rows = [
        (i, "a", [float((i * 7 + j) % 13) for j in range(VECTOR_DIM)])
        for i in range(1, 61)
    ]
    table = db.create_table("t", _vector_reader(rows))
    table.set_unenforced_primary_key("id")
    table.create_index(
        "vector", config=IvfPq(num_partitions=1, num_sub_vectors=2, num_bits=4)
    )
    # Spec with NO maintained indexes: the base vector index's catch-up is untracked,
    # so the scanner rejects rather than risk dropping compacted-but-unindexed rows.
    table.set_lsm_write_spec(LsmWriteSpec.unsharded().with_maintained_indexes([]))
    with pytest.raises(Exception, match="maintained"):
        table.search([1.0] * VECTOR_DIM).to_arrow()


def test_lsm_read_fts_optimized_index_not_rejected(tmp_path):
    db = lancedb.connect(tmp_path, read_consistency_interval=timedelta(seconds=0))
    table = db.create_table("t", _fts_reader([(i, "quick fox") for i in range(1, 6)]))
    table.set_unenforced_primary_key("id")
    table.create_index("text", config=FTS())
    table.add(_fts_reader([(i, "lazy fox") for i in range(6, 11)]))
    table.optimize()  # may split the FTS index into multiple physical segments
    name = table.list_indices()[0].name
    table.set_lsm_write_spec(LsmWriteSpec.unsharded().with_maintained_indexes([name]))
    # Multiple physical segments of one logical index must not be miscounted as
    # multiple indexes and rejected.
    ids = set(
        table.search("fox", query_type="fts", fts_columns="text")
        .limit(20)
        .to_arrow()["id"]
        .to_pylist()
    )
    assert ids == set(range(1, 11))
