# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

from dataclasses import dataclass
from functools import singledispatch
import sys
from typing import Callable, Iterator, Optional
from lancedb.arrow import to_arrow
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.dataset as ds

from .pydantic import LanceModel

# pyarrow's default scanner settings are tuned for narrow rows. For wide rows
# (e.g. embedding columns) they buffer a huge read-ahead window in host memory
# and can OOM the client during bulk ingestion. We size the scanner so the
# estimated in-flight memory stays within a budget, while leaving narrow
# datasets on pyarrow's defaults (no throughput regression).
_SCAN_MEMORY_BUDGET_BYTES = 1024 * 1024 * 1024  # ~1 GiB in-flight target
_TARGET_BATCH_BYTES = 16 * 1024 * 1024  # ~16 MiB per batch
_MIN_BATCH_ROWS = 512
# pyarrow defaults (see arrow/dataset ScanOptions); we never exceed these.
_PA_DEFAULT_BATCH_ROWS = 131_072
_PA_DEFAULT_BATCH_READAHEAD = 16
_PA_DEFAULT_FRAGMENT_READAHEAD = 4
# Read-ahead used for wide rows. pyarrow reads a whole parquet row group at a
# time and keeps `batch_readahead` of them resident, so read-ahead depth (not
# batch size) dominates peak memory for wide data; keep both small but leave a
# little prefetch for throughput. Tuned empirically against embedding datasets.
_WIDE_BATCH_READAHEAD = 2
_WIDE_FRAGMENT_READAHEAD = 1
# Estimate for variable-width columns (string/binary/list) whose true width is
# unknown from the schema alone. Only needs to be large enough to flag "wide".
_VARIABLE_WIDTH_ESTIMATE = 128
# Rows peeked from a rescannable source to refine the list-length guess for
# variable-length list columns (e.g. embeddings stored as `list<float32>`
# instead of `list<float32, N>`), whose per-row width the schema can't tell us.
_SAMPLE_ROWS = 10


def _observed_list_length(sample: pa.ChunkedArray) -> Optional[int]:
    """Average element count per row observed in a list/large_list sample."""
    if len(sample) == 0:
        return None
    mean = pc.mean(pc.list_value_length(sample)).as_py()
    return None if mean is None else max(1, round(mean))


def _estimate_field_width(
    dtype: pa.DataType, sample: Optional[pa.ChunkedArray] = None
) -> int:
    if pa.types.is_fixed_size_list(dtype):
        return dtype.list_size * _estimate_field_width(dtype.value_type)
    if pa.types.is_struct(dtype):
        return sum(
            _estimate_field_width(
                dtype.field(i).type,
                pc.struct_field(sample, [i]) if sample is not None else None,
            )
            for i in range(dtype.num_fields)
        )
    if pa.types.is_dictionary(dtype):
        return _estimate_field_width(dtype.value_type)
    if pa.types.is_fixed_size_binary(dtype):
        return dtype.byte_width
    if pa.types.is_boolean(dtype):
        return 1
    if (pa.types.is_list(dtype) or pa.types.is_large_list(dtype)) and (
        sample is not None
    ):
        observed_length = _observed_list_length(sample)
        if observed_length is not None:
            return observed_length * _estimate_field_width(dtype.value_type)
    # Fixed-width scalars (ints, floats, temporal, decimal) expose bit_width;
    # variable-width types (string, binary, list, map, ...) raise ValueError.
    try:
        return max(1, dtype.bit_width // 8)
    except (ValueError, AttributeError):
        return _VARIABLE_WIDTH_ESTIMATE


def _estimate_bytes_per_row(
    schema: pa.Schema, sample: Optional[pa.Table] = None
) -> int:
    return max(
        1,
        sum(
            _estimate_field_width(
                field.type, sample.column(field.name) if sample is not None else None
            )
            for field in schema
        ),
    )


def _sample_head(head: Callable[..., pa.Table]) -> Optional[pa.Table]:
    """Best-effort peek at a few rows to refine the bytes-per-row estimate.

    Uses a tight batch size and no read-ahead so the peek itself can't trigger
    the wide-row memory blowup this module exists to avoid. Returns None (fall
    back to the schema-only estimate) if sampling isn't possible for any
    reason, e.g. an empty dataset.
    """
    try:
        sample = head(
            _SAMPLE_ROWS,
            batch_size=_SAMPLE_ROWS,
            batch_readahead=1,
            fragment_readahead=1,
        )
    except Exception:
        return None
    return sample if sample.num_rows > 0 else None


def _bounded_scanner_kwargs(
    schema: pa.Schema, sample: Optional[pa.Table] = None
) -> dict:
    """Scanner kwargs that cap in-flight memory for wide rows.

    Narrow datasets keep pyarrow's defaults unchanged (no throughput
    regression). For wide rows (e.g. embedding columns) pyarrow's default
    read-ahead buffers many large batches/row-groups at once, which can OOM the
    client during bulk ingestion, so we shrink the batch size and read-ahead to
    keep the estimated in-flight memory near the budget.

    Read-ahead (not just batch size) has to drop: pyarrow reads a whole parquet
    row group at a time and keeps `batch_readahead`/`fragment_readahead` of them
    resident, so a small batch size alone still pins large row-group buffers.

    `sample`, if given, is a small (see `_SAMPLE_ROWS`) table of rows from the
    source used to refine the estimate for variable-length list columns (e.g.
    embeddings stored without a fixed size), whose width the schema alone
    can't tell us.
    """
    bytes_per_row = _estimate_bytes_per_row(schema, sample)

    # If pyarrow's defaults already stay within budget, leave them alone so
    # narrow datasets keep their throughput. A "unit" of in-flight memory is one
    # default-sized batch, held `batch_readahead + fragment_readahead` deep.
    default_in_flight = (
        _PA_DEFAULT_BATCH_ROWS
        * bytes_per_row
        * (_PA_DEFAULT_BATCH_READAHEAD + _PA_DEFAULT_FRAGMENT_READAHEAD)
    )
    if default_in_flight <= _SCAN_MEMORY_BUDGET_BYTES:
        return {}

    # Wide rows: cap batch bytes and pull read-ahead down so only a couple of
    # large row-group buffers are resident at once.
    batch_size = min(
        _PA_DEFAULT_BATCH_ROWS,
        max(_MIN_BATCH_ROWS, _TARGET_BATCH_BYTES // bytes_per_row),
    )
    return {
        "batch_size": batch_size,
        "batch_readahead": _WIDE_BATCH_READAHEAD,
        "fragment_readahead": _WIDE_FRAGMENT_READAHEAD,
    }


@dataclass
class Scannable:
    schema: pa.Schema
    num_rows: Optional[int]
    # Factory function to create a new reader each time (supports re-scanning)
    reader: Callable[[], pa.RecordBatchReader]
    # Whether reader can be called more than once. For example, an iterator can
    # only be consumed once, while a DataFrame can be converted to a new reader
    # each time.
    rescannable: bool = True


@singledispatch
def to_scannable(data) -> Scannable:
    # Fallback: try iterable protocol
    if hasattr(data, "__iter__"):
        return _from_iterable(iter(data))
    raise NotImplementedError(f"to_scannable not implemented for type {type(data)}")


@to_scannable.register(pa.RecordBatchReader)
def _from_reader(data: pa.RecordBatchReader) -> Scannable:
    # RecordBatchReader can only be consumed once - not rescannable
    return Scannable(
        schema=data.schema, num_rows=None, reader=lambda: data, rescannable=False
    )


@to_scannable.register(pa.RecordBatch)
def _from_batch(data: pa.RecordBatch) -> Scannable:
    return Scannable(
        schema=data.schema,
        num_rows=data.num_rows,
        reader=lambda: pa.RecordBatchReader.from_batches(data.schema, [data]),
    )


@to_scannable.register(pa.Table)
def _from_table(data: pa.Table) -> Scannable:
    return Scannable(schema=data.schema, num_rows=data.num_rows, reader=data.to_reader)


@to_scannable.register(ds.Dataset)
def _from_dataset(data: ds.Dataset) -> Scannable:
    sample = _sample_head(data.head)
    scanner_kwargs = _bounded_scanner_kwargs(data.schema, sample)
    return Scannable(
        schema=data.schema,
        num_rows=data.count_rows(),
        reader=lambda: data.scanner(**scanner_kwargs).to_reader(),
    )


@to_scannable.register(ds.Scanner)
def _from_scanner(data: ds.Scanner) -> Scannable:
    # Scanner can only be consumed once - not rescannable
    return Scannable(
        schema=data.projected_schema,
        num_rows=None,
        reader=data.to_reader,
        rescannable=False,
    )


@to_scannable.register(list)
def _from_list(data: list) -> Scannable:
    if not data:
        raise ValueError("Cannot create table from empty list without a schema")
    table = to_arrow(data)
    return Scannable(
        schema=table.schema, num_rows=table.num_rows, reader=table.to_reader
    )


@to_scannable.register(dict)
def _from_dict(data: dict) -> Scannable:
    raise ValueError(
        "Cannot create or add rows from a single dictionary. "
        "Use a list of dictionaries instead."
    )


@to_scannable.register(LanceModel)
def _from_lance_model(data: LanceModel) -> Scannable:
    raise ValueError("Cannot add a single LanceModel to a table. Use a list.")


def _from_iterable(data: Iterator) -> Scannable:
    first_item = next(data, None)
    if first_item is None:
        raise ValueError("Cannot create table from empty iterator")
    first = to_arrow(first_item)
    schema = first.schema

    def iter():
        yield from first.to_batches()
        for item in data:
            batch = to_arrow(item)
            if batch.schema != schema:
                try:
                    batch = batch.cast(schema)
                except pa.lib.ArrowInvalid:
                    raise ValueError(
                        f"Input iterator yielded a batch with schema that "
                        f"does not match the schema of other batches.\n"
                        f"Expected:\n{schema}\nGot:\n{batch.schema}"
                    )
            yield from batch.to_batches()

    reader = pa.RecordBatchReader.from_batches(schema, iter())
    return to_scannable(reader)


_registered_modules: set[str] = set()


def _register_optional_converters():
    """Register converters for optional dependencies that are already imported."""

    if "pandas" in sys.modules and "pandas" not in _registered_modules:
        _registered_modules.add("pandas")
        import pandas as pd

        @to_arrow.register(pd.DataFrame)
        def _arrow_from_pandas(data: pd.DataFrame) -> pa.Table:
            table = pa.Table.from_pandas(data, preserve_index=False)
            return table.replace_schema_metadata(None)

        @to_scannable.register(pd.DataFrame)
        def _from_pandas(data: pd.DataFrame) -> Scannable:
            return to_scannable(_arrow_from_pandas(data))

    if "polars" in sys.modules and "polars" not in _registered_modules:
        _registered_modules.add("polars")
        import polars as pl

        @to_arrow.register(pl.DataFrame)
        def _arrow_from_polars(data: pl.DataFrame) -> pa.Table:
            return data.to_arrow()

        @to_scannable.register(pl.DataFrame)
        def _from_polars(data: pl.DataFrame) -> Scannable:
            arrow = data.to_arrow()
            return Scannable(
                schema=arrow.schema, num_rows=len(data), reader=arrow.to_reader
            )

        @to_scannable.register(pl.LazyFrame)
        def _from_polars_lazy(data: pl.LazyFrame) -> Scannable:
            arrow = data.collect().to_arrow()
            return Scannable(
                schema=arrow.schema, num_rows=arrow.num_rows, reader=arrow.to_reader
            )

    if "datasets" in sys.modules and "datasets" not in _registered_modules:
        _registered_modules.add("datasets")
        from datasets import Dataset as HFDataset
        from datasets import DatasetDict as HFDatasetDict

        @to_scannable.register(HFDataset)
        def _from_hf_dataset(data: HFDataset) -> Scannable:
            table = data.data.table  # Access underlying Arrow table
            return Scannable(
                schema=table.schema, num_rows=len(data), reader=table.to_reader
            )

        @to_scannable.register(HFDatasetDict)
        def _from_hf_dataset_dict(data: HFDatasetDict) -> Scannable:
            # HuggingFace DatasetDict: combine all splits with a 'split' column
            schema = data[list(data.keys())[0]].features.arrow_schema
            if "split" not in schema.names:
                schema = schema.append(pa.field("split", pa.string()))

            def gen():
                for split_name, dataset in data.items():
                    for batch in dataset.data.to_batches():
                        split_arr = pa.array(
                            [split_name] * len(batch), type=pa.string()
                        )
                        yield pa.RecordBatch.from_arrays(
                            list(batch.columns) + [split_arr], schema=schema
                        )

            total_rows = sum(len(dataset) for dataset in data.values())
            return Scannable(
                schema=schema,
                num_rows=total_rows,
                reader=lambda: pa.RecordBatchReader.from_batches(schema, gen()),
            )

    if "lance" in sys.modules and "lance" not in _registered_modules:
        _registered_modules.add("lance")
        import lance

        @to_scannable.register(lance.LanceDataset)
        def _from_lance(data: lance.LanceDataset) -> Scannable:
            sample = _sample_head(data.head)
            scanner_kwargs = _bounded_scanner_kwargs(data.schema, sample)
            return Scannable(
                schema=data.schema,
                num_rows=data.count_rows(),
                reader=lambda: data.scanner(**scanner_kwargs).to_reader(),
            )


# Register on module load
_register_optional_converters()
