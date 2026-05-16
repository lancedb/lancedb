# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

"""Tests for Table.set_unenforced_primary_key."""

from datetime import timedelta

import lancedb
import pyarrow as pa
import pytest


def _connect(path):
    return lancedb.connect(path, read_consistency_interval=timedelta(seconds=0))


def test_set_unenforced_primary_key_validates(tmp_path):
    schema = pa.schema([pa.field("id", pa.utf8(), nullable=False)])
    table = _connect(tmp_path).create_table(
        "t",
        pa.RecordBatchReader.from_batches(
            schema,
            [
                pa.RecordBatch.from_arrays(
                    [pa.array(["seed"], type=pa.utf8())], schema=schema
                )
            ],
        ),
    )

    # Unknown column.
    with pytest.raises(Exception, match="not found"):
        table.set_unenforced_primary_key("nonexistent")

    # Unsupported dtype (Float32 not in the supported set).
    bad_schema = pa.schema([pa.field("id", pa.float32(), nullable=False)])
    bad_table = _connect(tmp_path / "bad").create_table(
        "bad",
        pa.RecordBatchReader.from_batches(
            bad_schema,
            [
                pa.RecordBatch.from_arrays(
                    [pa.array([1.0], type=pa.float32())], schema=bad_schema
                )
            ],
        ),
    )
    with pytest.raises(Exception, match="not supported"):
        bad_table.set_unenforced_primary_key("id")


def test_set_unenforced_primary_key_multi_column(tmp_path):
    schema = pa.schema(
        [
            pa.field("a", pa.utf8(), nullable=False),
            pa.field("b", pa.int64(), nullable=False),
        ]
    )
    table = _connect(tmp_path).create_table(
        "t",
        pa.RecordBatchReader.from_batches(
            schema,
            [
                pa.RecordBatch.from_arrays(
                    [pa.array(["x"], type=pa.utf8()), pa.array([1], type=pa.int64())],
                    schema=schema,
                )
            ],
        ),
    )

    # Single column via bare string.
    table.set_unenforced_primary_key("a")
    # Single column via list.
    table.set_unenforced_primary_key(["a"])
    # Composite key — order must be preserved.
    table.set_unenforced_primary_key(["a", "b"])
    # Empty input rejected.
    with pytest.raises(Exception, match="at least one"):
        table.set_unenforced_primary_key([])
    # Duplicates rejected.
    with pytest.raises(Exception, match="duplicate"):
        table.set_unenforced_primary_key(["a", "a"])
