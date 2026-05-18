# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

from functools import singledispatch
from typing import List, Optional, Tuple, Union

from lancedb.pydantic import LanceModel, model_to_dict
import pyarrow as pa

from ._lancedb import RecordBatchStream



def _has_struct_type(data_type):
    """Check if a data type contains a struct type (directly or nested)."""
    if pa.types.is_struct(data_type):
        return True
    if pa.types.is_list(data_type) or pa.types.is_large_list(data_type):
        return _has_struct_type(data_type.value_type)
    if pa.types.is_fixed_size_list(data_type):
        return _has_struct_type(data_type.value_type)
    return False


def _fix_struct_nulls(value, data_type, array, idx):
    """Recursively fix a single value from a struct array.

    If the array element at *idx* is null, return None regardless of what
    ``to_pylist()`` produced.  Otherwise walk the struct children and
    recursively fix any nested struct / list-of-struct fields.
    """
    if not array.is_valid(idx):
        return None
    if value is None:
        return None
    for j, sub_field in enumerate(data_type):
        if _has_struct_type(sub_field.type):
            child_array = array.field(j)
            value[sub_field.name] = _fix_value(
                value[sub_field.name], sub_field.type, child_array, idx
            )
    return value


def _fix_list_nulls(value, data_type, array, idx):
    """Fix null struct values inside a list element."""
    if not array.is_valid(idx):
        return None
    if value is None:
        return None

    value_type = data_type.value_type
    offsets = array.offsets.to_pylist()
    start = offsets[idx]
    values_array = array.values

    for k in range(len(value)):
        value[k] = _fix_value(value[k], value_type, values_array, start + k)
    return value


def _fix_value(value, data_type, array, idx):
    """Dispatch to the right null-fixer based on Arrow data type."""
    if pa.types.is_struct(data_type):
        return _fix_struct_nulls(value, data_type, array, idx)
    if pa.types.is_list(data_type) or pa.types.is_large_list(data_type):
        if _has_struct_type(data_type.value_type):
            return _fix_list_nulls(value, data_type, array, idx)
    return value


def _fix_column(column, field):
    """Fix null struct values in a single table column."""
    py_values = column.to_pylist()
    offset = 0
    for chunk in column.chunks:
        for i in range(len(chunk)):
            py_values[offset + i] = _fix_value(
                py_values[offset + i], field.type, chunk, i
            )
        offset += len(chunk)
    return py_values


def table_to_pylist(table):
    """Convert a PyArrow Table to a list of dicts, correctly handling null structs.

    PyArrow's built-in ``to_pylist()`` converts null struct values into dicts
    populated with default values (empty strings, zeros, etc.) instead of
    ``None``.  This function uses Arrow validity bitmaps to restore ``None``
    for every null struct -- and for null struct items inside lists -- at any
    nesting depth.

    Parameters
    ----------
    table : pa.Table
        The table to convert.

    Returns
    -------
    list[dict]
        List of dictionaries with correct null handling for struct fields.
    """
    if not any(_has_struct_type(field.type) for field in table.schema):
        return table.to_pylist()

    n_rows = len(table)
    result = [{} for _ in range(n_rows)]

    for i, field in enumerate(table.schema):
        column = table.column(i)
        if _has_struct_type(field.type):
            values = _fix_column(column, field)
        else:
            values = column.to_pylist()
        for row_idx in range(n_rows):
            result[row_idx][field.name] = values[row_idx]

    return result


class AsyncRecordBatchReader:
    """
    An async iterator over a stream of RecordBatches.

    Also allows access to the schema of the stream
    """

    def __init__(
        self,
        inner: Union[RecordBatchStream, pa.Table],
        max_batch_length: Optional[int] = None,
    ):
        """

        Attributes
        ----------
        schema : pa.Schema
            The schema of the batches produced by the stream.
            Accessing the schema does not consume any data from the stream
        """
        if isinstance(inner, pa.Table):
            self._inner = self._async_iter_from_table(inner, max_batch_length)
            self.schema: pa.Schema = inner.schema
        elif isinstance(inner, RecordBatchStream):
            self._inner = inner
            self.schema: pa.Schema = inner.schema
        else:
            raise TypeError("inner must be a RecordBatchStream or a Table")

    async def read_all(self) -> List[pa.RecordBatch]:
        """
        Read all the record batches from the stream

        This consumes the entire stream and returns a list of record batches

        If there are a lot of results this may consume a lot of memory
        """
        return [batch async for batch in self]

    def __aiter__(self):
        return self

    async def __anext__(self) -> pa.RecordBatch:
        return await self._inner.__anext__()

    @staticmethod
    async def _async_iter_from_table(
        table: pa.Table, max_batch_length: Optional[int] = None
    ):
        """
        Create an AsyncRecordBatchReader from a Table

        This is useful when you have a Table that you want to iterate
        over asynchronously
        """
        batches = table.to_batches(max_chunksize=max_batch_length)
        for batch in batches:
            yield batch


def peek_reader(
    reader: pa.RecordBatchReader,
) -> Tuple[pa.RecordBatch, pa.RecordBatchReader]:
    if not isinstance(reader, pa.RecordBatchReader):
        raise TypeError("reader must be a RecordBatchReader")
    batch = reader.read_next_batch()

    def all_batches():
        yield batch
        yield from reader

    return batch, pa.RecordBatchReader.from_batches(batch.schema, all_batches())


@singledispatch
def to_arrow(data) -> pa.Table:
    """Convert a single data object to a pa.Table."""
    raise NotImplementedError(f"to_arrow not implemented for type {type(data)}")


@to_arrow.register(pa.RecordBatch)
def _arrow_from_batch(data: pa.RecordBatch) -> pa.Table:
    return pa.Table.from_batches([data])


@to_arrow.register(pa.Table)
def _arrow_from_table(data: pa.Table) -> pa.Table:
    return data


@to_arrow.register(list)
def _arrow_from_list(data: list) -> pa.Table:
    if not data:
        raise ValueError("Cannot create table from empty list without a schema")

    if isinstance(data[0], LanceModel):
        schema = data[0].__class__.to_arrow_schema()
        dicts = [model_to_dict(d) for d in data]
        return pa.Table.from_pylist(dicts, schema=schema)

    return pa.Table.from_pylist(data)
