# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

import numpy as np
import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.parquet as pq

from lancedb.scannable import (
    _PA_DEFAULT_BATCH_ROWS,
    _WIDE_BATCH_READAHEAD,
    _WIDE_FRAGMENT_READAHEAD,
    _bounded_scanner_kwargs,
    _estimate_bytes_per_row,
    to_scannable,
)


def test_estimate_bytes_per_row():
    # fixed-width scalars
    assert _estimate_bytes_per_row(pa.schema([("a", pa.int64())])) == 8
    assert (
        _estimate_bytes_per_row(pa.schema([("a", pa.int32()), ("b", pa.float64())]))
        == 12
    )
    assert _estimate_bytes_per_row(pa.schema([("a", pa.bool_())])) == 1
    # fixed-size list (embedding) dominates
    assert (
        _estimate_bytes_per_row(pa.schema([("v", pa.list_(pa.float32(), 768))]))
        == 768 * 4
    )
    # struct sums its children
    struct = pa.struct([("x", pa.int32()), ("y", pa.int32())])
    assert _estimate_bytes_per_row(pa.schema([("s", struct)])) == 8
    # variable-width columns get a flat estimate, not zero
    assert _estimate_bytes_per_row(pa.schema([("s", pa.string())])) > 0


def test_bounded_scanner_kwargs_narrow_uses_defaults():
    # Narrow rows stay on pyarrow defaults (empty kwargs) so throughput is
    # unchanged.
    for schema in [
        pa.schema([("a", pa.int64()), ("b", pa.int32()), ("c", pa.string())]),
        pa.schema([("a", pa.int64()), ("t", pa.string()), ("u", pa.string())]),
        # a 100-dim float32 vector is still under the per-row budget
        pa.schema([("id", pa.int64()), ("v", pa.list_(pa.float32(), 100))]),
    ]:
        assert _bounded_scanner_kwargs(schema) == {}, schema


def test_bounded_scanner_kwargs_wide_is_bounded():
    schema = pa.schema(
        [
            ("uid", pa.string()),
            ("img", pa.list_(pa.float32(), 768)),
            ("txt", pa.list_(pa.float32(), 768)),
        ]
    )
    kwargs = _bounded_scanner_kwargs(schema)
    assert kwargs, "wide schema should be throttled"
    assert kwargs["batch_readahead"] == _WIDE_BATCH_READAHEAD
    assert kwargs["fragment_readahead"] == _WIDE_FRAGMENT_READAHEAD
    # batch is capped well below the pyarrow default for wide rows
    assert kwargs["batch_size"] < _PA_DEFAULT_BATCH_ROWS


def _write_wide_dataset(path, *, files=2, rows_per_file=20_000, dim=768):
    rng = np.random.default_rng(0)
    for i in range(files):
        emb = rng.standard_normal((rows_per_file, dim), dtype=np.float32)
        table = pa.table(
            {
                "uid": pa.array([f"{i}_{j}" for j in range(rows_per_file)]),
                "vec": pa.FixedSizeListArray.from_arrays(
                    pa.array(emb.reshape(-1)), dim
                ),
            }
        )
        pq.write_table(table, f"{path}/part-{i}.parquet")


def test_dataset_reader_respects_bounded_batch_size(tmp_path):
    # The Dataset path should stream small batches for wide rows, not pyarrow's
    # 131072-row default, and still return every row.
    _write_wide_dataset(str(tmp_path))
    dataset = ds.dataset(str(tmp_path), format="parquet")

    expected = _bounded_scanner_kwargs(dataset.schema)["batch_size"]

    scannable = to_scannable(dataset)
    assert scannable.rescannable
    assert scannable.num_rows == 40_000

    total = 0
    for batch in scannable.reader():
        assert batch.num_rows <= expected
        total += batch.num_rows
    assert total == 40_000

    # factory can be called again (rescannable)
    assert sum(b.num_rows for b in scannable.reader()) == 40_000
