# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

import numpy as np
import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.parquet as pq

from lancedb.scannable import (
    _PA_DEFAULT_BATCH_ROWS,
    _SAMPLE_ROWS,
    _VARIABLE_WIDTH_ESTIMATE,
    _WIDE_BATCH_READAHEAD,
    _WIDE_FRAGMENT_READAHEAD,
    _bounded_scanner_kwargs,
    _estimate_bytes_per_row,
    _sample_head,
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


def test_estimate_bytes_per_row_uses_sample_for_variable_length_lists():
    # A vector column without a fixed size (e.g. `list<float32>` instead of
    # `list<float32, 768>`) has no width the schema alone can tell us.
    schema = pa.schema([("v", pa.list_(pa.float32()))])
    assert _estimate_bytes_per_row(schema) == _VARIABLE_WIDTH_ESTIMATE

    sample = pa.table({"v": pa.array([[0.0] * 768], type=pa.list_(pa.float32()))})
    assert _estimate_bytes_per_row(schema, sample) == 768 * 4


def test_estimate_bytes_per_row_sample_ignores_missing_or_null_lists():
    schema = pa.schema([("v", pa.list_(pa.float32()))])
    # an all-null sample column can't tell us anything either
    sample = pa.table({"v": pa.array([None], type=pa.list_(pa.float32()))})
    assert _estimate_bytes_per_row(schema, sample) == _VARIABLE_WIDTH_ESTIMATE


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


def test_bounded_scanner_kwargs_variable_length_list_needs_sample():
    # Without a sample, a variable-length (not fixed-size) vector column looks
    # narrow because its true width is unknown from the schema alone.
    schema = pa.schema([("uid", pa.string()), ("vec", pa.list_(pa.float32()))])
    assert _bounded_scanner_kwargs(schema) == {}

    sample = pa.table(
        {
            "uid": pa.array(["a"]),
            "vec": pa.array([[0.0] * 768], type=pa.list_(pa.float32())),
        }
    )
    kwargs = _bounded_scanner_kwargs(schema, sample)
    assert kwargs, "sample should reveal the wide vector column"
    assert kwargs["batch_size"] < _PA_DEFAULT_BATCH_ROWS


def _write_wide_dataset(
    path, *, files=2, rows_per_file=20_000, dim=768, fixed_size=True
):
    rng = np.random.default_rng(0)
    for i in range(files):
        emb = rng.standard_normal((rows_per_file, dim), dtype=np.float32)
        vec_type = pa.list_(pa.float32(), dim) if fixed_size else pa.list_(pa.float32())
        vec_array = (
            pa.FixedSizeListArray.from_arrays(pa.array(emb.reshape(-1)), dim)
            if fixed_size
            else pa.array(emb.tolist(), type=vec_type)
        )
        table = pa.table(
            {
                "uid": pa.array([f"{i}_{j}" for j in range(rows_per_file)]),
                "vec": vec_array,
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


def test_dataset_reader_samples_variable_length_list_width(tmp_path):
    # A vector column stored without a fixed size (e.g. produced by tools that
    # don't tag list columns with their length) is invisible to the
    # schema-only estimate, so `to_scannable` must peek a sample of rows to
    # detect that it's wide and bound the scanner accordingly.
    _write_wide_dataset(str(tmp_path), fixed_size=False)
    dataset = ds.dataset(str(tmp_path), format="parquet")

    schema_only_kwargs = _bounded_scanner_kwargs(dataset.schema)
    assert schema_only_kwargs == {}, "schema alone can't see the list width"

    scannable = to_scannable(dataset)
    assert scannable.rescannable
    assert scannable.num_rows == 40_000

    total = 0
    for batch in scannable.reader():
        assert batch.num_rows < _PA_DEFAULT_BATCH_ROWS
        total += batch.num_rows
    assert total == 40_000


def test_sample_head_is_bounded_rows(tmp_path):
    # The peek itself must not read the whole dataset.
    _write_wide_dataset(str(tmp_path), files=1, rows_per_file=1000, fixed_size=False)
    dataset = ds.dataset(str(tmp_path), format="parquet")

    sample = _sample_head(dataset.head)
    assert sample.num_rows == _SAMPLE_ROWS


def test_sample_head_returns_none_for_empty_dataset(tmp_path):
    table = pa.table({"v": pa.array([], type=pa.list_(pa.float32()))})
    pq.write_table(table, f"{tmp_path}/empty.parquet")
    dataset = ds.dataset(str(tmp_path), format="parquet")

    assert _sample_head(dataset.head) is None
