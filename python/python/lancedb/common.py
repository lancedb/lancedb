# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors


from pathlib import Path
from typing import Iterable, List, Optional, Union

import numpy as np
import pyarrow as pa
import pyarrow.dataset

from .dependencies import _check_for_pandas, pandas as pd

DATA = Union[List[dict], "pd.DataFrame", pa.Table, Iterable[pa.RecordBatch]]
VEC = Union[list, np.ndarray, pa.Array, pa.ChunkedArray]
URI = Union[str, Path]
VECTOR_COLUMN_NAME = "vector"


class Credential(str):
    """Credential field"""

    def __repr__(self) -> str:
        return "********"

    def __str__(self) -> str:
        return "********"


def sanitize_uri(uri: URI) -> str:
    return str(uri)


def _casting_recordbatch_iter(
    input_iter: Iterable[pa.RecordBatch], schema: pa.Schema
) -> Iterable[pa.RecordBatch]:
    """
    Wrapper around an iterator of record batches. If the batches don't match the
    schema, try to cast them to the schema. If that fails, raise an error.

    This is helpful for users who might have written the iterator with default
    data types in PyArrow, but specified more specific types in the schema. For
    example, PyArrow defaults to float64 for floating point types, but Lance
    uses float32 for vectors.
    """
    for batch in input_iter:
        if not isinstance(batch, pa.RecordBatch):
            raise TypeError(f"Expected RecordBatch, got {type(batch)}")
        if batch.schema != schema:
            try:
                # RecordBatch doesn't have a cast method, but table does.
                batch = pa.Table.from_batches([batch]).cast(schema).to_batches()[0]
            except pa.lib.ArrowInvalid:
                raise ValueError(
                    f"Input RecordBatch iterator yielded a batch with schema that "
                    f"does not match the expected schema.\nExpected:\n{schema}\n"
                    f"Got:\n{batch.schema}"
                )
        yield batch


def data_to_reader(
    data: DATA, schema: Optional[pa.Schema] = None
) -> pa.RecordBatchReader:
    """Convert various types of input into a RecordBatchReader"""
    if _check_for_pandas(data) and isinstance(data, pd.DataFrame):
        return pa.Table.from_pandas(data, schema=schema).to_reader()
    elif isinstance(data, pa.Table):
        return data.to_reader()
    elif isinstance(data, pa.RecordBatch):
        return pa.Table.from_batches([data]).to_reader()
    # elif isinstance(data, LanceDataset):
    #     return data_obj.scanner().to_reader()
    elif isinstance(data, pa.dataset.Dataset):
        return pa.dataset.Scanner.from_dataset(data).to_reader()
    elif isinstance(data, pa.dataset.Scanner):
        return data.to_reader()
    elif isinstance(data, pa.RecordBatchReader):
        return data
    elif (
        type(data).__module__.startswith("polars")
        and data.__class__.__name__ == "DataFrame"
    ):
        return data.to_arrow().to_reader()
    # for other iterables, assume they are of type Iterable[RecordBatch]
    elif isinstance(data, Iterable):
        if schema is not None:
            data = _casting_recordbatch_iter(data, schema)
            return pa.RecordBatchReader.from_batches(schema, data)
        else:
            raise ValueError(
                "Must provide schema to write dataset from RecordBatch iterable"
            )
    else:
        raise TypeError(
            f"Unknown data type {type(data)}. "
            "Please check "
            "https://lancedb.github.io/lance/read_and_write.html "
            "to see supported types."
        )


def validate_schema(schema: pa.Schema):
    """
    Make sure the metadata is valid utf8
    """
    if schema.metadata is not None:
        _validate_metadata(schema.metadata)


def _validate_metadata(metadata: dict):
    """
    Make sure the metadata values are valid utf8 (can be nested)

    Raises ValueError if not valid utf8
    """
    for k, v in metadata.items():
        if isinstance(v, bytes):
            try:
                v.decode("utf8")
            except UnicodeDecodeError:
                raise ValueError(
                    f"Metadata key {k} is not valid utf8. "
                    "Consider base64 encode for generic binary metadata."
                )
        elif isinstance(v, dict):
            _validate_metadata(v)
