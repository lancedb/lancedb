# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

"""Tests for Table.set_unenforced_primary_key."""

from datetime import timedelta

import lancedb
import pyarrow as pa
import pytest


def _empty_table(path, schema):
    db = lancedb.connect(path, read_consistency_interval=timedelta(seconds=0))
    return db.create_table("t", schema=schema)


def test_set_unenforced_primary_key_accepts_string_or_one_element_list(tmp_path):
    schema = pa.schema([pa.field("id", pa.int64(), nullable=False)])

    # Bare string.
    table = _empty_table(tmp_path / "s", schema)
    table.set_unenforced_primary_key("id")

    # One-element list.
    table = _empty_table(tmp_path / "l", schema)
    table.set_unenforced_primary_key(["id"])


def test_set_unenforced_primary_key_rejects_compound_and_empty(tmp_path):
    table = _empty_table(
        tmp_path,
        pa.schema(
            [
                pa.field("a", pa.utf8(), nullable=False),
                pa.field("b", pa.int64(), nullable=False),
            ]
        ),
    )
    # Compound keys are not supported.
    with pytest.raises(Exception, match="compound"):
        table.set_unenforced_primary_key(["a", "b"])
    # Empty input.
    with pytest.raises(Exception, match="required"):
        table.set_unenforced_primary_key([])


def test_set_unenforced_primary_key_is_immutable(tmp_path):
    table = _empty_table(
        tmp_path,
        pa.schema(
            [
                pa.field("a", pa.utf8(), nullable=False),
                pa.field("b", pa.int64(), nullable=False),
            ]
        ),
    )
    table.set_unenforced_primary_key("a")
    # The primary key cannot be changed or re-set once installed.
    with pytest.raises(Exception, match="already set"):
        table.set_unenforced_primary_key("b")
    with pytest.raises(Exception, match="already set"):
        table.set_unenforced_primary_key("a")


def test_set_unenforced_primary_key_validates(tmp_path):
    table = _empty_table(
        tmp_path / "t", pa.schema([pa.field("id", pa.utf8(), nullable=False)])
    )
    # Unknown column.
    with pytest.raises(Exception, match="not found"):
        table.set_unenforced_primary_key("nonexistent")

    # Unsupported dtype (Float32 not in the supported set).
    bad = _empty_table(
        tmp_path / "bad", pa.schema([pa.field("id", pa.float32(), nullable=False)])
    )
    with pytest.raises(Exception, match="not supported"):
        bad.set_unenforced_primary_key("id")
