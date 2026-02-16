# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

from dataclasses import dataclass
from functools import singledispatch
from typing import Callable, Iterator, Optional
import sys
import pyarrow as pa
import pyarrow.dataset as ds

from .pydantic import LanceModel, model_to_dict


@dataclass
class Scannable:
    schema: pa.Schema
    num_rows: Optional[int]
    # Factory function to create a new reader each time (supports re-scanning)
    reader: Callable[[], pa.RecordBatchReader]
    # Whether calling reader() multiple times yields fresh data
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

    # Handle list of LanceModels
    if isinstance(data[0], LanceModel):
        schema = data[0].__class__.to_arrow_schema()
        dicts = [model_to_dict(d) for d in data]
        table = pa.Table.from_pylist(dicts, schema=schema)
        return Scannable(
            schema=table.schema, num_rows=len(data), reader=table.to_reader
        )

    # Handle list of RecordBatches
    if isinstance(data[0], pa.RecordBatch):
        table = pa.Table.from_batches(data)
        return Scannable(
            schema=table.schema, num_rows=table.num_rows, reader=table.to_reader
        )

    # Handle list of dicts (most common case)
    table = pa.Table.from_pylist(data)
    return Scannable(schema=table.schema, num_rows=len(data), reader=table.to_reader)


@to_scannable.register(dict)
def _from_dict(data: dict) -> Scannable:
    raise ValueError("Cannot add a single dictionary to a table. Use a list.")


def _from_iterable(data: Iterator) -> Scannable:
    # Consume iterator into table (no row count hint)
    table = pa.Table.from_pylist(list(data))
    return Scannable(
        schema=table.schema, num_rows=table.num_rows, reader=table.to_reader
    )


_registered_modules: set[str] = set()


def _register_optional_converters():
    """Register converters for optional dependencies that are already imported."""

    if "pandas" in sys.modules and "pandas" not in _registered_modules:
        _registered_modules.add("pandas")
        import pandas as pd

        @to_scannable.register(pd.DataFrame)
        def _from_pandas(data: pd.DataFrame) -> Scannable:
            # Remove pandas metadata to avoid schema conflicts
            table = pa.Table.from_pandas(data, preserve_index=False)
            table = table.replace_schema_metadata(None)
            return Scannable(
                schema=table.schema, num_rows=len(data), reader=table.to_reader
            )

    if "polars" in sys.modules and "polars" not in _registered_modules:
        _registered_modules.add("polars")
        import polars as pl

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
