# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

from dataclasses import dataclass
from functools import singledispatch
import sys
from typing import Callable, Iterator, Optional
from lancedb.arrow import to_arrow
import pyarrow as pa
import pyarrow.dataset as ds

from .pydantic import LanceModel


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
    return Scannable(
        schema=data.schema,
        num_rows=data.count_rows(),
        reader=lambda: data.scanner().to_reader(),
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
    raise ValueError("Cannot add a single dictionary to a table. Use a list.")


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
            return Scannable(
                schema=data.schema,
                num_rows=data.count_rows(),
                reader=lambda: data.scanner().to_reader(),
            )


# Register on module load
_register_optional_converters()
