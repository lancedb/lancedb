#  Copyright 2023 LanceDB Developers
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

import os
import pathlib
from typing import Optional

from lancedb.table import (
    _coerce_to_table,
    _sanitize_data,
    _sanitize_schema,
    _sanitize_vector_column,
)
import pyarrow as pa
import pandas as pd
import polars as pl
import pytest
import lancedb
from lancedb.util import get_uri_scheme, join_uri, value_to_sql
from utils import exception_output


def test_normalize_uri():
    uris = [
        "relative/path",
        "/absolute/path",
        "file:///absolute/path",
        "s3://bucket/path",
        "gs://bucket/path",
        "c:\\windows\\path",
    ]
    schemes = ["file", "file", "file", "s3", "gs", "file"]

    for uri, expected_scheme in zip(uris, schemes):
        parsed_scheme = get_uri_scheme(uri)
        assert parsed_scheme == expected_scheme


def test_join_uri_remote():
    schemes = ["s3", "az", "gs"]
    for scheme in schemes:
        expected = f"{scheme}://bucket/path/to/table.lance"
        base_uri = f"{scheme}://bucket/path/to/"
        parts = ["table.lance"]
        assert join_uri(base_uri, *parts) == expected

        base_uri = f"{scheme}://bucket"
        parts = ["path", "to", "table.lance"]
        assert join_uri(base_uri, *parts) == expected


# skip this test if on windows
@pytest.mark.skipif(os.name == "nt", reason="Windows paths are not POSIX")
def test_join_uri_posix():
    for base in [
        # relative path
        "relative/path",
        "relative/path/",
        # an absolute path
        "/absolute/path",
        "/absolute/path/",
        # a file URI
        "file:///absolute/path",
        "file:///absolute/path/",
    ]:
        joined = join_uri(base, "table.lance")
        assert joined == str(pathlib.Path(base) / "table.lance")
        joined = join_uri(pathlib.Path(base), "table.lance")
        assert joined == pathlib.Path(base) / "table.lance"


# skip this test if not on windows
@pytest.mark.skipif(os.name != "nt", reason="Windows paths are not POSIX")
def test_local_join_uri_windows():
    # https://learn.microsoft.com/en-us/dotnet/standard/io/file-path-formats
    for base in [
        # windows relative path
        "relative\\path",
        "relative\\path\\",
        # windows absolute path from current drive
        "c:\\absolute\\path",
        # relative path from root of current drive
        "\\relative\\path",
    ]:
        joined = join_uri(base, "table.lance")
        assert joined == str(pathlib.Path(base) / "table.lance")
        joined = join_uri(pathlib.Path(base), "table.lance")
        assert joined == pathlib.Path(base) / "table.lance"


def test_value_to_sql_string(tmp_path):
    # Make sure we can convert Python string literals to SQL strings, even if
    # they contain characters meaningful in SQL, such as ' and \.
    values = ["anthony's", 'a "test" string', "anthony's \"favorite color\" wasn't red"]
    expected_values = [
        "'anthony''s'",
        "'a \"test\" string'",
        "'anthony''s \"favorite color\" wasn''t red'",
    ]

    for value, expected in zip(values, expected_values):
        assert value_to_sql(value) == expected

    # Also test we can roundtrip those strings through update.
    # This validates the query parser understands the strings we
    # are creating.
    db = lancedb.connect(tmp_path)
    table = db.create_table(
        "test",
        [{"search": value, "replace": "something"} for value in values],
    )
    for value in values:
        table.update(where=f"search = {value_to_sql(value)}", values={"replace": value})
        assert table.to_pandas().query("search == @value")["replace"].item() == value


@pytest.mark.parametrize(
    "input",
    [
        pa.array([[1.0, 2.0]]),
        pa.chunked_array([[[1.0, 2.0]], [[3.0, 4.0]]]),
    ],
)
@pytest.mark.parametrize(
    "value_type",
    [
        pa.float16(),
        pa.float32(),
        pa.float64(),
    ],
)
def test_sanitize_vectors_cast(input, value_type):
    data = pa.table({"vector": input})
    schema = pa.schema({"vector": pa.list_(value_type, 2)})
    output = _sanitize_vector_column(
        data,
        vector_column_name="vector",
        table_schema=schema,
    )
    assert output["vector"].type == schema.field("vector").type


@pytest.mark.parametrize("on_bad_vectors", ["error", "drop", "fill", "null"])
def test_sanitize_vectors_jagged(on_bad_vectors):
    vector = pa.array([[1.0, 2.0], [3.0]])
    output_type = pa.list_(pa.float64(), 2)
    schema = pa.schema({"vector": output_type})
    data = pa.table({"vector": vector})

    if on_bad_vectors == "error":
        with pytest.raises(ValueError) as e:
            output = _sanitize_vector_column(
                data,
                vector_column_name="vector",
                table_schema=schema,
                on_bad_vectors=on_bad_vectors,
            )
        output = exception_output(e)
        assert output == (
            "ValueError: Vector column 'vector' has variable length vectors. Set "
            "on_bad_vectors='drop' to remove them, set on_bad_vectors='fill' "
            "and fill_value=<value> to replace them, or set on_bad_vectors='null' "
            "to replace them with null."
        )
        return
    else:
        output = _sanitize_vector_column(
            data,
            vector_column_name="vector",
            table_schema=schema,
            on_bad_vectors=on_bad_vectors,
            fill_value=42.0,
        )

    if on_bad_vectors == "drop":
        expected = pa.array([[1.0, 2.0]], type=output_type)
    elif on_bad_vectors == "fill":
        expected = pa.array([[1.0, 2.0], [42.0, 42.0]], type=output_type)
    elif on_bad_vectors == "null":
        expected = pa.array([[1.0, 2.0], None], type=output_type)

    assert output["vector"].combine_chunks() == expected


@pytest.mark.parametrize("on_bad_vectors", ["error", "drop", "fill", "null"])
def test_sanitize_vectors_nan(on_bad_vectors):
    vector = pa.array([[1.0, float("nan")], [3.0, 4.0]])
    output_type = pa.list_(pa.float64(), 2)
    schema = pa.schema({"vector": output_type})
    data = pa.table({"vector": vector})

    if on_bad_vectors == "error":
        with pytest.raises(ValueError) as e:
            output = _sanitize_vector_column(
                data,
                vector_column_name="vector",
                table_schema=schema,
                on_bad_vectors=on_bad_vectors,
            )
        output = exception_output(e)
        assert output == (
            "ValueError: Vector column 'vector' has NaNs. Set "
            "on_bad_vectors='drop' to remove them, set on_bad_vectors='fill' "
            "and fill_value=<value> to replace them, or set on_bad_vectors='null' "
            "to replace them with null."
        )
        return
    else:
        output = _sanitize_vector_column(
            data,
            vector_column_name="vector",
            table_schema=schema,
            on_bad_vectors=on_bad_vectors,
            fill_value=42.0,
        )

    if on_bad_vectors == "drop":
        expected = pa.array([[3.0, 4.0]], type=output_type)
    elif on_bad_vectors == "fill":
        expected = pa.array([[42.0, 42.0], [3.0, 4.0]], type=output_type)
    elif on_bad_vectors == "null":
        expected = pa.array([None, [3.0, 4.0]], type=output_type)

    assert output["vector"].combine_chunks() == expected


def test_sanitize_vectors_noop():
    # ChunkedArray should be preserved as-is
    vector = pa.chunked_array(
        [[[1.0, 2.0], [3.0, 4.0]]], type=pa.list_(pa.float64(), 2)
    )
    schema = pa.schema({"vector": pa.list_(pa.float64(), 2)})
    data = pa.table({"vector": vector})
    output = _sanitize_vector_column(
        data, vector_column_name="vector", table_schema=schema
    )
    assert output["vector"] == vector


def test_sanitize_schema():
    # Converts default vector column
    data = pa.Table.from_pylist(
        [{"vector": [0.0] * 10}],
        schema=pa.schema({"vector": pa.list_(pa.float64())}),
    )
    expected = pa.Table.from_pylist(
        [{"vector": [0.0] * 10}],
        schema=pa.schema({"vector": pa.list_(pa.float32(), 10)}),
    )
    output = _sanitize_schema(data)
    assert output == expected

    # Converts all vector columns, if schema supplied
    data = pa.table(
        {
            "vec1": [[0.0] * 10],
            "vec2": [[0.0] * 10],
            "vec3": pa.array([[0.0] * 10]).cast(pa.list_(pa.float16())),
        },
        schema=pa.schema(
            {
                "vec1": pa.list_(pa.float64()),
                "vec2": pa.list_(pa.float32()),
                "vec3": pa.list_(pa.float16()),
            }
        ),
    )
    schema = pa.schema(
        {
            "vec1": pa.list_(pa.float64(), 10),
            "vec2": pa.list_(pa.float32(), 10),
            "vec3": pa.list_(pa.float16(), 10),
        }
    )
    expected = pa.table(
        {
            "vec1": [[0.0] * 10],
            "vec2": [[0.0] * 10],
            "vec3": pa.array([[0.0] * 10]).cast(pa.list_(pa.float16(), 10)),
        },
        schema=schema,
    )
    output = _sanitize_schema(data, schema)
    assert output == expected

    # Can sanitize to subschema
    schema = pa.schema(
        {
            "vec1": pa.list_(pa.float64(), 10),
            "vec2": pa.list_(pa.float32(), 10),
            "vec3": pa.list_(pa.float16(), 10),
        }
    )
    data = pa.table(
        {
            "vec2": pa.array([[0.0] * 10]),
        }
    )
    expected = pa.table(
        {
            "vec2": pa.array([[0.0] * 10]).cast(pa.list_(pa.float32(), 10)),
        }
    )
    output = _sanitize_schema(data, schema)


# TODO: add dataset, scanner, RecordBatchReader, iterable of batches,
# LanceDataset, huggingface, LanceModel
@pytest.mark.parametrize(
    "data",
    [
        [{"a": 1, "b": 2}],
        pa.RecordBatch.from_pylist([{"a": 1, "b": 2}]),
        pa.table({"a": [1], "b": [2]}),
        pd.DataFrame({"a": [1], "b": [2]}),
        pl.DataFrame({"a": [1], "b": [2]}),
        pl.LazyFrame({"a": [1], "b": [2]}),
    ],
    ids=[
        "rows",
        "pa.RecordBatch",
        "pa.Table",
        "pd.DataFrame",
        "pl.DataFrame",
        "pl.LazyFrame",
    ],
)
def test_coerce_to_table(data):
    # Infers schema correctly
    expected = pa.table({"a": [1], "b": [2]})
    output = _coerce_to_table(data)
    assert output == expected

    # Uses provided schema
    schema = pa.schema({"a": pa.int64(), "b": pa.int64()})
    expected = pa.table({"a": [1], "b": [2]}, schema=schema)
    output = _coerce_to_table(data, schema=schema)
    assert output == expected


@pytest.mark.parametrize(
    "data",
    [
        [{"a": 1, "b": 2}],
        pa.RecordBatch.from_pylist([{"a": 1, "b": 2}]),
        pd.DataFrame({"a": [1], "b": [2]}),
        pl.DataFrame({"a": [1], "b": [2]}),
    ],
    ids=["rows", "pa.RecordBatch", "pd.DataFrame", "pl.DataFrame"],
)
@pytest.mark.parametrize(
    "schema",
    [
        None,
        pa.schema({"a": pa.int64(), "b": pa.int32()}),
        pa.schema({"a": pa.int64(), "b": pa.int32(), "c": pa.int64()}),
    ],
    ids=["infer", "explicit", "subschema"],
)
@pytest.mark.parametrize("with_embedding", [True, False])
def test_sanitize_data(
    data,
    schema: Optional[pa.Schema],
    with_embedding: bool,
):
    # TODO: embeddings
    if with_embedding:
        metadata = {}  # TODO
    else:
        metadata = None

    if schema is not None:
        to_remove = schema.get_field_index("c")
        if to_remove >= 0:
            expected_schema = schema.remove(to_remove)
        else:
            expected_schema = schema
    else:
        expected_schema = None
    expected = pa.table(
        {
            "a": [1],
            "b": [2],
        },
        schema=expected_schema,
    )

    output_data, output_schema = _sanitize_data(
        data,
        schema=schema,
        metadata=metadata,
        allow_subschema=True,
    )

    assert output_data == expected

    if schema is not None:
        # If we supplied a schema, we expect the output schema to be the same as
        # the input schema, except with the field we removed.
        assert output_schema == schema
    else:
        # Otherwise, we expect it to match the data schema.
        assert output_schema == output_data.schema
