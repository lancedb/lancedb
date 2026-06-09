# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

"""Regression matrix for nested field support across LanceDB Python APIs.

Covers the lifecycle described in lancedb/lancedb#3406:
  - Nested scalar, vector, and FTS index creation with full dotted paths
  - list_indices / index_stats return canonical full paths (not leaf names)
  - search, filter, append, optimize behaviour
  - Field-name edge cases: mixed case, literal-dot field names, same-name leaves
  - Both sync and async Python table APIs

The matrix uses the following field-name variants from the acceptance criteria:
  - rowId              (camelCase top-level)
  - `row-id`           (hyphenated top-level, escaped)
  - parent.`leaf.name` (struct leaf whose name contains a literal dot)
  - MetaData.userId    (mixed-case nested path)
  - `meta-data`.`user-id`  (hyphenated struct with hyphenated leaf)

Note: Lance forbids top-level field names that contain a '.', so the literal-dot
edge case is exercised via a struct leaf field (parent.`leaf.name`) instead.
"""

from datetime import timedelta

import pyarrow as pa
import pytest
import pytest_asyncio

import lancedb
from lancedb.db import AsyncConnection, DBConnection
from lancedb.index import BTree, FTS, IvfPq


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

DIM = 8
# IvfPq requires at least num_partitions * 256 rows by default; keeping rows
# small means we must drop num_sub_vectors and num_partitions very low.
NROWS = 256


def _vec(row: int) -> list:
    return [float((row * DIM + i) % 256) for i in range(DIM)]


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def sync_db(tmp_path) -> DBConnection:
    return lancedb.connect(tmp_path)


@pytest_asyncio.fixture
async def async_db(tmp_path) -> AsyncConnection:
    return await lancedb.connect_async(
        tmp_path, read_consistency_interval=timedelta(seconds=0)
    )


# ---------------------------------------------------------------------------
# Schema / data builders
# ---------------------------------------------------------------------------


def _nested_scalar_schema() -> pa.Schema:
    """Schema with nested scalar fields covering the acceptance-criteria names.

    Top-level columns:
      - rowId       int32  (camelCase top-level)
      - row-id      int32  (hyphenated top-level name)
      - MetaData    struct{userId int32}   (mixed-case nested path)
      - meta-data   struct{user-id int32}  (hyphenated struct + hyphenated leaf)

    Lance disallows top-level field names that contain '.' (e.g. a field
    literally named 'a.b'), so that edge case is tested separately using
    _literal_dot_schema() below.
    """
    return pa.schema(
        [
            pa.field("rowId", pa.int32()),
            pa.field("row-id", pa.int32()),
            pa.field(
                "MetaData",
                pa.struct([pa.field("userId", pa.int32())]),
            ),
            pa.field(
                "meta-data",
                pa.struct([pa.field("user-id", pa.int32())]),
            ),
        ]
    )


def _nested_scalar_data(nrows: int = NROWS) -> pa.Table:
    schema = _nested_scalar_schema()
    return pa.table(
        {
            "rowId": pa.array(list(range(nrows)), pa.int32()),
            "row-id": pa.array(list(range(nrows)), pa.int32()),
            "MetaData": pa.array(
                [{"userId": i} for i in range(nrows)],
                type=pa.struct([pa.field("userId", pa.int32())]),
            ),
            "meta-data": pa.array(
                [{"user-id": i} for i in range(nrows)],
                type=pa.struct([pa.field("user-id", pa.int32())]),
            ),
        },
        schema=schema,
    )


def _literal_dot_schema() -> pa.Schema:
    """Schema where a struct *leaf* field is named with a literal dot.

    The path used in the index API is ``parent.`leaf.name` ``.
    """
    return pa.schema(
        [
            pa.field("id", pa.int32()),
            pa.field(
                "parent",
                pa.struct([pa.field("leaf.name", pa.int32())]),
            ),
        ]
    )


def _literal_dot_data(nrows: int = NROWS) -> pa.Table:
    parent_type = pa.struct([pa.field("leaf.name", pa.int32())])
    return pa.table(
        {
            "id": pa.array(list(range(nrows)), pa.int32()),
            "parent": pa.array(
                [{"leaf.name": i} for i in range(nrows)],
                type=parent_type,
            ),
        },
        schema=_literal_dot_schema(),
    )


def _same_leaf_schema() -> pa.Schema:
    return pa.schema(
        [
            pa.field("StructA", pa.struct([pa.field("userId", pa.int32())])),
            pa.field("StructB", pa.struct([pa.field("userId", pa.int32())])),
        ]
    )


def _same_leaf_data(nrows: int = NROWS) -> pa.Table:
    t = pa.struct([pa.field("userId", pa.int32())])
    return pa.table(
        {
            "StructA": pa.array([{"userId": i} for i in range(nrows)], type=t),
            "StructB": pa.array([{"userId": i * 10} for i in range(nrows)], type=t),
        },
        schema=_same_leaf_schema(),
    )


def _nested_vector_schema() -> pa.Schema:
    return pa.schema(
        [
            pa.field("id", pa.int32()),
            pa.field(
                "image",
                pa.struct(
                    [pa.field("embedding", pa.list_(pa.float32(), DIM))]
                ),
            ),
            pa.field(
                "MetaData",
                pa.struct([pa.field("userId", pa.int32())]),
            ),
        ]
    )


def _nested_vector_data(nrows: int = NROWS) -> pa.Table:
    embedding_type = pa.list_(pa.float32(), DIM)
    image_type = pa.struct([pa.field("embedding", embedding_type)])
    meta_type = pa.struct([pa.field("userId", pa.int32())])
    return pa.table(
        {
            "id": pa.array(list(range(nrows)), pa.int32()),
            "image": pa.array(
                [{"embedding": _vec(i)} for i in range(nrows)],
                type=image_type,
            ),
            "MetaData": pa.array(
                [{"userId": i} for i in range(nrows)],
                type=meta_type,
            ),
        },
        schema=_nested_vector_schema(),
    )


def _nested_fts_schema() -> pa.Schema:
    return pa.schema(
        [
            pa.field("id", pa.int32()),
            pa.field(
                "payload",
                pa.struct([pa.field("text", pa.utf8())]),
            ),
            pa.field(
                "MetaData",
                pa.struct([pa.field("userId", pa.int32())]),
            ),
        ]
    )


def _nested_fts_data(nrows: int = NROWS) -> pa.Table:
    words = ["alpha", "bravo", "charlie", "delta", "echo"]
    payload_type = pa.struct([pa.field("text", pa.utf8())])
    meta_type = pa.struct([pa.field("userId", pa.int32())])
    return pa.table(
        {
            "id": pa.array(list(range(nrows)), pa.int32()),
            "payload": pa.array(
                [{"text": words[i % len(words)]} for i in range(nrows)],
                type=payload_type,
            ),
            "MetaData": pa.array(
                [{"userId": i} for i in range(nrows)],
                type=meta_type,
            ),
        },
        schema=_nested_fts_schema(),
    )


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _columns_by_name_sync(tbl) -> dict:
    return {idx.name: idx.columns for idx in tbl.list_indices()}


async def _columns_by_name_async(tbl) -> dict:
    return {idx.name: idx.columns for idx in await tbl.list_indices()}


# ===========================================================================
# SYNC TESTS
# ===========================================================================
#
# The sync LanceTable API uses:
#   - create_scalar_index(column, ...)  for scalar (BTree/Bitmap/LabelList) indices
#   - create_fts_index(column, ...)     for full-text-search indices
#   - create_index(...)                 for vector indices (older positional API)
# ===========================================================================


class TestNestedScalarIndexSync:
    """Sync regression matrix for nested scalar (BTree) indices."""

    def test_top_level_camelcase_field(self, sync_db):
        """list_indices must return the full camelCase field name."""
        tbl = sync_db.create_table("t", _nested_scalar_data())
        tbl.create_scalar_index("rowId", index_type="BTREE", name="rowid_idx")
        col_map = _columns_by_name_sync(tbl)
        assert col_map["rowid_idx"] == ["rowId"], (
            "list_indices must return 'rowId', not a truncated leaf name"
        )

    def test_top_level_hyphenated_field_escaped(self, sync_db):
        """Top-level field 'row-id' (hyphenated) accessed via escaped path."""
        tbl = sync_db.create_table("t", _nested_scalar_data())
        tbl.create_scalar_index("`row-id`", index_type="BTREE", name="rowid_hyph_idx")
        col_map = _columns_by_name_sync(tbl)
        assert col_map["rowid_hyph_idx"] == ["`row-id`"], (
            "list_indices must return escaped path '`row-id`'"
        )

    def test_struct_leaf_literal_dot_field_escaped(self, sync_db):
        """Struct leaf with a literal-dot name: parent.`leaf.name`.

        The index listing must use the full escaped path, not just the leaf.
        """
        tbl = sync_db.create_table("t", _literal_dot_data())
        tbl.create_scalar_index(
            "parent.`leaf.name`", index_type="BTREE", name="leaf_dot_idx"
        )
        col_map = _columns_by_name_sync(tbl)
        assert col_map["leaf_dot_idx"] == ["parent.`leaf.name`"], (
            "list_indices must return 'parent.`leaf.name`', not just '`leaf.name`'"
        )

    def test_nested_mixed_case_path(self, sync_db):
        """Nested path MetaData.userId (mixed case) must appear as full path."""
        tbl = sync_db.create_table("t", _nested_scalar_data())
        tbl.create_scalar_index(
            "MetaData.userId", index_type="BTREE", name="metadata_userid_idx"
        )
        col_map = _columns_by_name_sync(tbl)
        assert col_map["metadata_userid_idx"] == ["MetaData.userId"], (
            "list_indices must return 'MetaData.userId', not leaf 'userId'"
        )

    def test_nested_hyphenated_path_escaped(self, sync_db):
        """`meta-data`.`user-id` path with both parts escaped."""
        tbl = sync_db.create_table("t", _nested_scalar_data())
        tbl.create_scalar_index(
            "`meta-data`.`user-id`", index_type="BTREE", name="metauid_idx"
        )
        col_map = _columns_by_name_sync(tbl)
        assert col_map["metauid_idx"] == ["`meta-data`.`user-id`"], (
            "list_indices must return '`meta-data`.`user-id`', not 'user-id'"
        )

    def test_filter_on_nested_mixed_case(self, sync_db):
        """WHERE filter on a nested dotted path works after index creation."""
        tbl = sync_db.create_table("t", _nested_scalar_data())
        tbl.create_scalar_index(
            "MetaData.userId", index_type="BTREE", name="metadata_userid_idx"
        )
        rows = tbl.search().where("MetaData.userId = 5").to_list()
        assert len(rows) == 1
        assert rows[0]["MetaData"]["userId"] == 5

    def test_append_and_list_indices_stable(self, sync_db):
        """After appending rows the index listing must remain unchanged."""
        tbl = sync_db.create_table("t", _nested_scalar_data())
        tbl.create_scalar_index(
            "MetaData.userId", index_type="BTREE", name="meta_uid_idx"
        )
        tbl.add(_nested_scalar_data(nrows=4))
        col_map = _columns_by_name_sync(tbl)
        assert col_map["meta_uid_idx"] == ["MetaData.userId"]

    def test_optimize_and_list_indices_stable(self, tmp_path):
        """After optimize the index listing must still show full paths."""
        db = lancedb.connect(tmp_path / "opt_db")
        tbl = db.create_table("t", _nested_scalar_data())
        tbl.create_scalar_index(
            "MetaData.userId", index_type="BTREE", name="meta_uid_idx"
        )
        tbl.add(_nested_scalar_data(nrows=4))
        tbl.optimize()
        col_map = _columns_by_name_sync(tbl)
        assert col_map["meta_uid_idx"] == ["MetaData.userId"]

    def test_same_name_leaves_are_distinct(self, sync_db):
        """Two structs sharing a leaf name must produce distinct index paths."""
        tbl = sync_db.create_table("same_leaf", _same_leaf_data())
        tbl.create_scalar_index(
            "StructA.userId", index_type="BTREE", name="a_userid_idx"
        )
        tbl.create_scalar_index(
            "StructB.userId", index_type="BTREE", name="b_userid_idx"
        )
        col_map = _columns_by_name_sync(tbl)
        assert col_map["a_userid_idx"] == ["StructA.userId"]
        assert col_map["b_userid_idx"] == ["StructB.userId"]

    def test_index_stats_canonical_path(self, sync_db):
        """index_stats round-trip: create on nested field, verify row count."""
        tbl = sync_db.create_table("t", _nested_scalar_data())
        tbl.create_scalar_index(
            "MetaData.userId", index_type="BTREE", name="meta_uid_idx"
        )
        stats = tbl.index_stats("meta_uid_idx")
        assert stats is not None
        assert stats.index_type == "BTREE"
        assert stats.num_indexed_rows == NROWS


class TestNestedVectorIndexSync:
    """Sync regression matrix for nested vector (IvfPq) indices."""

    def test_nested_vector_index_full_path(self, sync_db):
        """Listing after vector index creation must use the full dotted path."""
        tbl = sync_db.create_table("vt", _nested_vector_data())
        tbl.create_index(
            num_partitions=2,
            num_sub_vectors=2,
            vector_column_name="image.embedding",
            name="image_emb_idx",
        )
        col_map = _columns_by_name_sync(tbl)
        assert col_map["image_emb_idx"] == ["image.embedding"], (
            "list_indices must return 'image.embedding', not leaf 'embedding'"
        )

    def test_nested_vector_search(self, sync_db):
        """Vector search on nested embedding field must return results."""
        tbl = sync_db.create_table("vt", _nested_vector_data())
        tbl.create_index(
            num_partitions=2,
            num_sub_vectors=2,
            vector_column_name="image.embedding",
            name="image_emb_idx",
        )
        results = (
            tbl.search(_vec(0), vector_column_name="image.embedding")
            .limit(5)
            .to_list()
        )
        assert len(results) > 0

    def test_nested_vector_index_stats(self, sync_db):
        """index_stats for a nested vector index must reflect correct row count."""
        tbl = sync_db.create_table("vt", _nested_vector_data())
        tbl.create_index(
            num_partitions=2,
            num_sub_vectors=2,
            vector_column_name="image.embedding",
            name="image_emb_idx",
        )
        stats = tbl.index_stats("image_emb_idx")
        assert stats is not None
        assert stats.num_indexed_rows == NROWS

    def test_nested_vector_append_optimize(self, tmp_path):
        """After append and optimize the vector index listing must be stable."""
        db = lancedb.connect(tmp_path / "vec_opt_db")
        tbl = db.create_table("vt", _nested_vector_data())
        tbl.create_index(
            num_partitions=2,
            num_sub_vectors=2,
            vector_column_name="image.embedding",
            name="image_emb_idx",
        )
        tbl.add(_nested_vector_data(nrows=4))
        tbl.optimize()
        col_map = _columns_by_name_sync(tbl)
        assert col_map["image_emb_idx"] == ["image.embedding"]


class TestNestedFTSIndexSync:
    """Sync regression matrix for nested FTS indices."""

    def test_nested_fts_index_full_path(self, sync_db):
        """FTS index on payload.text must be listed with the full path."""
        tbl = sync_db.create_table("ft", _nested_fts_data())
        tbl.create_fts_index("payload.text", name="payload_text_idx")
        col_map = _columns_by_name_sync(tbl)
        assert col_map["payload_text_idx"] == ["payload.text"], (
            "list_indices must return 'payload.text', not leaf 'text'"
        )

    def test_nested_fts_search(self, sync_db):
        """FTS search on a nested text field must return correct results."""
        tbl = sync_db.create_table("ft", _nested_fts_data())
        tbl.create_fts_index("payload.text", name="payload_text_idx")
        results = (
            tbl.search("alpha", query_type="fts", fts_columns="payload.text")
            .limit(10)
            .to_list()
        )
        assert len(results) > 0
        assert all(row["payload"]["text"] == "alpha" for row in results)

    def test_nested_fts_append_optimize(self, tmp_path):
        """After append and optimize the FTS index listing must be stable."""
        db = lancedb.connect(tmp_path / "fts_opt_db")
        tbl = db.create_table("ft", _nested_fts_data())
        tbl.create_fts_index("payload.text", name="payload_text_idx")
        tbl.add(_nested_fts_data(nrows=4))
        tbl.optimize()
        col_map = _columns_by_name_sync(tbl)
        assert col_map["payload_text_idx"] == ["payload.text"]


# ===========================================================================
# ASYNC TESTS
# ===========================================================================
#
# The async AsyncTable API uses create_index(column, config=...) uniformly
# for scalar, vector, and FTS indices.
# ===========================================================================


class TestNestedScalarIndexAsync:
    """Async regression matrix for nested scalar (BTree) indices."""

    @pytest.mark.asyncio
    async def test_top_level_camelcase_field(self, async_db):
        """list_indices must return the full camelCase field name."""
        tbl = await async_db.create_table("t", _nested_scalar_data())
        await tbl.create_index("rowId", config=BTree(), name="rowid_idx")
        col_map = await _columns_by_name_async(tbl)
        assert col_map["rowid_idx"] == ["rowId"]

    @pytest.mark.asyncio
    async def test_top_level_hyphenated_field_escaped(self, async_db):
        """Hyphenated top-level field accessed via escaped path."""
        tbl = await async_db.create_table("t", _nested_scalar_data())
        await tbl.create_index("`row-id`", config=BTree(), name="rowid_hyph_idx")
        col_map = await _columns_by_name_async(tbl)
        assert col_map["rowid_hyph_idx"] == ["`row-id`"]

    @pytest.mark.asyncio
    async def test_struct_leaf_literal_dot_field_escaped(self, async_db):
        """Struct leaf with a literal-dot name: parent.`leaf.name`."""
        tbl = await async_db.create_table("t", _literal_dot_data())
        await tbl.create_index(
            "parent.`leaf.name`", config=BTree(), name="leaf_dot_idx"
        )
        col_map = await _columns_by_name_async(tbl)
        assert col_map["leaf_dot_idx"] == ["parent.`leaf.name`"]

    @pytest.mark.asyncio
    async def test_nested_mixed_case_path(self, async_db):
        """Mixed-case nested path MetaData.userId must appear as full path."""
        tbl = await async_db.create_table("t", _nested_scalar_data())
        await tbl.create_index(
            "MetaData.userId", config=BTree(), name="metadata_userid_idx"
        )
        col_map = await _columns_by_name_async(tbl)
        assert col_map["metadata_userid_idx"] == ["MetaData.userId"]

    @pytest.mark.asyncio
    async def test_nested_hyphenated_path_escaped(self, async_db):
        """`meta-data`.`user-id` path with both parts escaped."""
        tbl = await async_db.create_table("t", _nested_scalar_data())
        await tbl.create_index(
            "`meta-data`.`user-id`", config=BTree(), name="metauid_idx"
        )
        col_map = await _columns_by_name_async(tbl)
        assert col_map["metauid_idx"] == ["`meta-data`.`user-id`"]

    @pytest.mark.asyncio
    async def test_filter_on_nested_mixed_case(self, async_db):
        """WHERE filter on a nested dotted path works after index creation."""
        tbl = await async_db.create_table("t", _nested_scalar_data())
        await tbl.create_index(
            "MetaData.userId", config=BTree(), name="metadata_userid_idx"
        )
        rows = await tbl.query().where("MetaData.userId = 5").to_list()
        assert len(rows) == 1
        assert rows[0]["MetaData"]["userId"] == 5

    @pytest.mark.asyncio
    async def test_index_stats_canonical_path(self, async_db):
        """index_stats round-trip: create on nested field, verify stats."""
        tbl = await async_db.create_table("t", _nested_scalar_data())
        await tbl.create_index(
            "MetaData.userId", config=BTree(), name="meta_uid_idx"
        )
        stats = await tbl.index_stats("meta_uid_idx")
        assert stats is not None
        assert stats.index_type == "BTREE"
        assert stats.num_indexed_rows == NROWS

    @pytest.mark.asyncio
    async def test_append_and_list_indices_stable(self, async_db):
        """After appending rows the index listing must remain unchanged."""
        tbl = await async_db.create_table("t", _nested_scalar_data())
        await tbl.create_index(
            "MetaData.userId", config=BTree(), name="meta_uid_idx"
        )
        await tbl.add(_nested_scalar_data(nrows=4))
        col_map = await _columns_by_name_async(tbl)
        assert col_map["meta_uid_idx"] == ["MetaData.userId"]

    @pytest.mark.asyncio
    async def test_optimize_and_list_indices_stable(self, tmp_path):
        """After optimize the index listing must still show full paths."""
        db = await lancedb.connect_async(
            tmp_path / "opt_db", read_consistency_interval=timedelta(seconds=0)
        )
        tbl = await db.create_table("t", _nested_scalar_data())
        await tbl.create_index(
            "MetaData.userId", config=BTree(), name="meta_uid_idx"
        )
        await tbl.add(_nested_scalar_data(nrows=4))
        await tbl.optimize()
        col_map = await _columns_by_name_async(tbl)
        assert col_map["meta_uid_idx"] == ["MetaData.userId"]

    @pytest.mark.asyncio
    async def test_same_name_leaves_are_distinct(self, async_db):
        """Two structs sharing a leaf name must produce distinct index paths."""
        tbl = await async_db.create_table("same_leaf", _same_leaf_data())
        await tbl.create_index("StructA.userId", config=BTree(), name="a_userid_idx")
        await tbl.create_index("StructB.userId", config=BTree(), name="b_userid_idx")
        col_map = await _columns_by_name_async(tbl)
        assert col_map["a_userid_idx"] == ["StructA.userId"]
        assert col_map["b_userid_idx"] == ["StructB.userId"]


class TestNestedVectorIndexAsync:
    """Async regression matrix for nested vector (IvfPq) indices."""

    @pytest.mark.asyncio
    async def test_nested_vector_index_full_path(self, async_db):
        """Listing after vector index creation must use the full dotted path."""
        tbl = await async_db.create_table("vt", _nested_vector_data())
        await tbl.create_index(
            "image.embedding",
            config=IvfPq(num_partitions=2, num_sub_vectors=2),
            name="image_emb_idx",
        )
        col_map = await _columns_by_name_async(tbl)
        assert col_map["image_emb_idx"] == ["image.embedding"]

    @pytest.mark.asyncio
    async def test_nested_vector_search(self, async_db):
        """Vector search on nested embedding field must return results."""
        tbl = await async_db.create_table("vt", _nested_vector_data())
        await tbl.create_index(
            "image.embedding",
            config=IvfPq(num_partitions=2, num_sub_vectors=2),
            name="image_emb_idx",
        )
        results = (
            await tbl.query()
            .nearest_to(_vec(0))
            .column("image.embedding")
            .limit(5)
            .to_list()
        )
        assert len(results) > 0

    @pytest.mark.asyncio
    async def test_nested_vector_index_stats(self, async_db):
        """index_stats for a nested vector index must reflect correct row count."""
        tbl = await async_db.create_table("vt", _nested_vector_data())
        await tbl.create_index(
            "image.embedding",
            config=IvfPq(num_partitions=2, num_sub_vectors=2),
            name="image_emb_idx",
        )
        stats = await tbl.index_stats("image_emb_idx")
        assert stats is not None
        assert stats.num_indexed_rows == NROWS

    @pytest.mark.asyncio
    async def test_nested_vector_append_optimize(self, tmp_path):
        """After append and optimize the vector index listing must be stable."""
        db = await lancedb.connect_async(
            tmp_path / "vec_opt_db", read_consistency_interval=timedelta(seconds=0)
        )
        tbl = await db.create_table("vt", _nested_vector_data())
        await tbl.create_index(
            "image.embedding",
            config=IvfPq(num_partitions=2, num_sub_vectors=2),
            name="image_emb_idx",
        )
        await tbl.add(_nested_vector_data(nrows=4))
        await tbl.optimize()
        col_map = await _columns_by_name_async(tbl)
        assert col_map["image_emb_idx"] == ["image.embedding"]


class TestNestedFTSIndexAsync:
    """Async regression matrix for nested FTS indices."""

    @pytest.mark.asyncio
    async def test_nested_fts_index_full_path(self, async_db):
        """FTS index on payload.text must be listed with the full path."""
        tbl = await async_db.create_table("ft", _nested_fts_data())
        await tbl.create_index("payload.text", config=FTS(), name="payload_text_idx")
        col_map = await _columns_by_name_async(tbl)
        assert col_map["payload_text_idx"] == ["payload.text"]

    @pytest.mark.asyncio
    async def test_nested_fts_search(self, async_db):
        """FTS search on a nested text field must return correct results."""
        tbl = await async_db.create_table("ft", _nested_fts_data())
        await tbl.create_index("payload.text", config=FTS(), name="payload_text_idx")
        results = (
            await tbl.query()
            .nearest_to_text("alpha", columns="payload.text")
            .limit(10)
            .to_list()
        )
        assert len(results) > 0
        assert all(row["payload"]["text"] == "alpha" for row in results)

    @pytest.mark.asyncio
    async def test_nested_fts_append_optimize(self, tmp_path):
        """After append and optimize the FTS index listing must be stable."""
        db = await lancedb.connect_async(
            tmp_path / "fts_opt_db", read_consistency_interval=timedelta(seconds=0)
        )
        tbl = await db.create_table("ft", _nested_fts_data())
        await tbl.create_index("payload.text", config=FTS(), name="payload_text_idx")
        await tbl.add(_nested_fts_data(nrows=4))
        await tbl.optimize()
        col_map = await _columns_by_name_async(tbl)
        assert col_map["payload_text_idx"] == ["payload.text"]
