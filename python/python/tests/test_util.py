# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors


import os
import pathlib
from typing import Optional

import lance
from lancedb.conftest import MockTextEmbeddingFunction
from lancedb.embeddings.base import EmbeddingFunctionConfig
from lancedb.embeddings.registry import EmbeddingFunctionRegistry
from lancedb.table import (
    _append_vector_columns,
    _cast_to_target_schema,
    _handle_bad_vectors,
    _into_pyarrow_reader,
    _sanitize_data,
    _infer_target_schema,
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


def test_append_vector_columns():
    registry = EmbeddingFunctionRegistry.get_instance()
    registry.register("test")(MockTextEmbeddingFunction)
    conf = EmbeddingFunctionConfig(
        source_column="text",
        vector_column="vector",
        function=MockTextEmbeddingFunction.create(),
    )
    metadata = registry.get_table_metadata([conf])

    schema = pa.schema(
        {
            "text": pa.string(),
            "vector": pa.list_(pa.float64(), 10),
        }
    )
    data = pa.table(
        {
            "text": ["hello"],
            "vector": [None],  # Replaces null
        },
        schema=schema,
    )
    output = _append_vector_columns(
        data.to_reader(),
        schema,  # metadata passed separate from schema
        metadata=metadata,
    ).read_all()
    assert output.schema == schema
    assert output["vector"].null_count == 0

    # Adds if missing
    data = pa.table({"text": ["hello"]})
    output = _append_vector_columns(
        data.to_reader(),
        schema.with_metadata(metadata),
    ).read_all()
    assert output.schema == schema
    assert output["vector"].null_count == 0

    # doesn't embed if already there
    data = pa.table(
        {
            "text": ["hello"],
            "vector": [[42.0] * 10],
        },
        schema=schema,
    )
    output = _append_vector_columns(
        data.to_reader(),
        schema.with_metadata(metadata),
    ).read_all()
    assert output == data  # No change

    # No provided schema
    data = pa.table(
        {
            "text": ["hello"],
        }
    )
    output = _append_vector_columns(
        data.to_reader(),
        metadata=metadata,
    ).read_all()
    expected_schema = pa.schema(
        {
            "text": pa.string(),
            "vector": pa.list_(pa.float32(), 10),
        }
    )
    assert output.schema == expected_schema
    assert output["vector"].null_count == 0


@pytest.mark.parametrize("on_bad_vectors", ["error", "drop", "fill", "null"])
def test_handle_bad_vectors_jagged(on_bad_vectors):
    vector = pa.array([[1.0, 2.0], [3.0], [4.0, 5.0]])
    schema = pa.schema({"vector": pa.list_(pa.float64())})
    data = pa.table({"vector": vector}, schema=schema)

    if on_bad_vectors == "error":
        with pytest.raises(ValueError) as e:
            output = _handle_bad_vectors(
                data.to_reader(),
                on_bad_vectors=on_bad_vectors,
            ).read_all()
        output = exception_output(e)
        assert output == (
            "ValueError: Vector column 'vector' has variable length vectors. Set "
            "on_bad_vectors='drop' to remove them, set on_bad_vectors='fill' "
            "and fill_value=<value> to replace them, or set on_bad_vectors='null' "
            "to replace them with null."
        )
        return
    else:
        output = _handle_bad_vectors(
            data.to_reader(),
            on_bad_vectors=on_bad_vectors,
            fill_value=42.0,
        ).read_all()

    if on_bad_vectors == "drop":
        expected = pa.array([[1.0, 2.0], [4.0, 5.0]])
    elif on_bad_vectors == "fill":
        expected = pa.array([[1.0, 2.0], [42.0, 42.0], [4.0, 5.0]])
    elif on_bad_vectors == "null":
        expected = pa.array([[1.0, 2.0], None, [4.0, 5.0]])

    assert output["vector"].combine_chunks() == expected


@pytest.mark.parametrize("on_bad_vectors", ["error", "drop", "fill", "null"])
def test_handle_bad_vectors_nan(on_bad_vectors):
    vector = pa.array([[1.0, float("nan")], [3.0, 4.0]])
    data = pa.table({"vector": vector})

    if on_bad_vectors == "error":
        with pytest.raises(ValueError) as e:
            output = _handle_bad_vectors(
                data.to_reader(),
                on_bad_vectors=on_bad_vectors,
            ).read_all()
        output = exception_output(e)
        assert output == (
            "ValueError: Vector column 'vector' has NaNs. Set "
            "on_bad_vectors='drop' to remove them, set on_bad_vectors='fill' "
            "and fill_value=<value> to replace them, or set on_bad_vectors='null' "
            "to replace them with null."
        )
        return
    else:
        output = _handle_bad_vectors(
            data.to_reader(),
            on_bad_vectors=on_bad_vectors,
            fill_value=42.0,
        ).read_all()

    if on_bad_vectors == "drop":
        expected = pa.array([[3.0, 4.0]])
    elif on_bad_vectors == "fill":
        expected = pa.array([[42.0, 42.0], [3.0, 4.0]])
    elif on_bad_vectors == "null":
        expected = pa.array([None, [3.0, 4.0]])

    assert output["vector"].combine_chunks() == expected


def test_handle_bad_vectors_noop():
    # ChunkedArray should be preserved as-is
    vector = pa.chunked_array(
        [[[1.0, 2.0], [3.0, 4.0]]], type=pa.list_(pa.float64(), 2)
    )
    data = pa.table({"vector": vector})
    output = _handle_bad_vectors(data.to_reader()).read_all()
    assert output["vector"] == vector


class TestModel(lancedb.pydantic.LanceModel):
    a: Optional[int]
    b: Optional[int]


# TODO: huggingface,
@pytest.mark.parametrize(
    "data",
    [
        lambda: [{"a": 1, "b": 2}],
        lambda: pa.RecordBatch.from_pylist([{"a": 1, "b": 2}]),
        lambda: pa.table({"a": [1], "b": [2]}),
        lambda: pa.table({"a": [1], "b": [2]}).to_reader(),
        lambda: iter(pa.table({"a": [1], "b": [2]}).to_batches()),
        lambda: (
            lance.write_dataset(
                pa.table({"a": [1], "b": [2]}),
                "memory://test",
            )
        ),
        lambda: (
            lance.write_dataset(
                pa.table({"a": [1], "b": [2]}),
                "memory://test",
            ).scanner()
        ),
        lambda: pd.DataFrame({"a": [1], "b": [2]}),
        lambda: pl.DataFrame({"a": [1], "b": [2]}),
        lambda: pl.LazyFrame({"a": [1], "b": [2]}),
        lambda: [TestModel(a=1, b=2)],
    ],
    ids=[
        "rows",
        "pa.RecordBatch",
        "pa.Table",
        "pa.RecordBatchReader",
        "batch_iter",
        "lance.LanceDataset",
        "lance.LanceScanner",
        "pd.DataFrame",
        "pl.DataFrame",
        "pl.LazyFrame",
        "pydantic",
    ],
)
def test_into_pyarrow_table(data):
    expected = pa.table({"a": [1], "b": [2]})
    output = _into_pyarrow_reader(data()).read_all()
    assert output == expected


def test_infer_target_schema():
    example = pa.schema(
        {
            "vec1": pa.list_(pa.float64(), 2),
            "vector": pa.list_(pa.float64()),
        }
    )
    data = pa.table(
        {
            "vec1": [[0.0] * 2],
            "vector": [[0.0] * 2],
        },
        schema=example,
    )
    expected = pa.schema(
        {
            "vec1": pa.list_(pa.float64(), 2),
            "vector": pa.list_(pa.float32(), 2),
        }
    )
    output, _ = _infer_target_schema(data.to_reader())
    assert output == expected

    # Handle large list and use modal size
    # Most vectors are of length 2, so we should infer that as the target dimension
    example = pa.schema(
        {
            "vector": pa.large_list(pa.float64()),
        }
    )
    data = pa.table(
        {
            "vector": [[0.0] * 2, [0.0], [0.0] * 2],
        },
        schema=example,
    )
    expected = pa.schema(
        {
            "vector": pa.list_(pa.float32(), 2),
        }
    )
    output, _ = _infer_target_schema(data.to_reader())
    assert output == expected

    # ignore if not list
    example = pa.schema(
        {
            "vector": pa.float64(),
        }
    )
    data = pa.table(
        {
            "vector": [0.0],
        },
        schema=example,
    )
    expected = example
    output, _ = _infer_target_schema(data.to_reader())
    assert output == expected


@pytest.mark.parametrize(
    "data",
    [
        [{"id": 1, "text": "hello"}],
        pa.RecordBatch.from_pylist([{"id": 1, "text": "hello"}]),
        pd.DataFrame({"id": [1], "text": ["hello"]}),
        pl.DataFrame({"id": [1], "text": ["hello"]}),
    ],
    ids=["rows", "pa.RecordBatch", "pd.DataFrame", "pl.DataFrame"],
)
@pytest.mark.parametrize(
    "schema",
    [
        None,
        pa.schema(
            {
                "id": pa.int32(),
                "text": pa.string(),
                "vector": pa.list_(pa.float32(), 10),
            }
        ),
        pa.schema(
            {
                "id": pa.int64(),
                "text": pa.string(),
                "vector": pa.list_(pa.float32(), 10),
                "extra": pa.int64(),
            }
        ),
    ],
    ids=["infer", "explicit", "subschema"],
)
@pytest.mark.parametrize("with_embedding", [True, False])
def test_sanitize_data(
    data,
    schema: Optional[pa.Schema],
    with_embedding: bool,
):
    if with_embedding:
        registry = EmbeddingFunctionRegistry.get_instance()
        registry.register("test")(MockTextEmbeddingFunction)
        conf = EmbeddingFunctionConfig(
            source_column="text",
            vector_column="vector",
            function=MockTextEmbeddingFunction.create(),
        )
        metadata = registry.get_table_metadata([conf])
    else:
        metadata = None

    if schema is not None:
        to_remove = schema.get_field_index("extra")
        if to_remove >= 0:
            expected_schema = schema.remove(to_remove)
        else:
            expected_schema = schema
    else:
        expected_schema = pa.schema(
            {
                "id": pa.int64(),
                "text": pa.large_utf8()
                if isinstance(data, pl.DataFrame)
                else pa.string(),
                "vector": pa.list_(pa.float32(), 10),
            }
        )

    if not with_embedding:
        to_remove = expected_schema.get_field_index("vector")
        if to_remove >= 0:
            expected_schema = expected_schema.remove(to_remove)

    expected = pa.table(
        {
            "id": [1],
            "text": ["hello"],
            "vector": [[0.0] * 10],
        },
        schema=expected_schema,
    )

    output_data = _sanitize_data(
        data,
        target_schema=schema,
        metadata=metadata,
        allow_subschema=True,
    ).read_all()

    assert output_data == expected


def test_cast_to_target_schema():
    original_schema = pa.schema(
        {
            "id": pa.int32(),
            "struct": pa.struct(
                [
                    pa.field("a", pa.int32()),
                ]
            ),
            "vector": pa.list_(pa.float64()),
            "vec1": pa.list_(pa.float64(), 2),
            "vec2": pa.list_(pa.float32(), 2),
        }
    )
    data = pa.table(
        {
            "id": [1],
            "struct": [{"a": 1}],
            "vector": [[0.0] * 2],
            "vec1": [[0.0] * 2],
            "vec2": [[0.0] * 2],
        },
        schema=original_schema,
    )

    target = pa.schema(
        {
            "id": pa.int64(),
            "struct": pa.struct(
                [
                    pa.field("a", pa.int64()),
                ]
            ),
            "vector": pa.list_(pa.float32(), 2),
            "vec1": pa.list_(pa.float32(), 2),
            "vec2": pa.list_(pa.float32(), 2),
        }
    )
    output = _cast_to_target_schema(data.to_reader(), target)
    expected = pa.table(
        {
            "id": [1],
            "struct": [{"a": 1}],
            "vector": [[0.0] * 2],
            "vec1": [[0.0] * 2],
            "vec2": [[0.0] * 2],
        },
        schema=target,
    )

    # Data can be a subschema of the target
    target = pa.schema(
        {
            "id": pa.int64(),
            "struct": pa.struct(
                [
                    pa.field("a", pa.int64()),
                    # Additional nested field
                    pa.field("b", pa.int64()),
                ]
            ),
            "vector": pa.list_(pa.float32(), 2),
            "vec1": pa.list_(pa.float32(), 2),
            "vec2": pa.list_(pa.float32(), 2),
            # Additional field
            "extra": pa.int64(),
        }
    )
    with pytest.raises(Exception):
        _cast_to_target_schema(data.to_reader(), target)
    output = _cast_to_target_schema(
        data.to_reader(), target, allow_subschema=True
    ).read_all()
    expected_schema = pa.schema(
        {
            "id": pa.int64(),
            "struct": pa.struct(
                [
                    pa.field("a", pa.int64()),
                ]
            ),
            "vector": pa.list_(pa.float32(), 2),
            "vec1": pa.list_(pa.float32(), 2),
            "vec2": pa.list_(pa.float32(), 2),
        }
    )
    expected = pa.table(
        {
            "id": [1],
            "struct": [{"a": 1}],
            "vector": [[0.0] * 2],
            "vec1": [[0.0] * 2],
            "vec2": [[0.0] * 2],
        },
        schema=expected_schema,
    )
    assert output == expected


def test_sanitize_data_stream():
    # Make sure we don't collect the whole stream when running sanitize_data
    schema = pa.schema({"a": pa.int32()})

    def stream():
        yield pa.record_batch([pa.array([1, 2, 3])], schema=schema)
        raise ValueError("error")

    reader = pa.RecordBatchReader.from_batches(schema, stream())

    output = _sanitize_data(reader)

    first = next(output)
    assert first == pa.record_batch([pa.array([1, 2, 3])], schema=schema)

    with pytest.raises(ValueError):
        next(output)


def test_infer_vector_column_query_single_vector():
    """Test that infer_vector_column_query returns the single vector column."""
    from lancedb.util import infer_vector_column_query

    schema = pa.schema(
        {"id": pa.int64(), "text": pa.string(), "vector": pa.list_(pa.float32(), 10)}
    )

    result = infer_vector_column_query(schema)
    assert result == "vector"


def test_infer_vector_column_query_no_vector_columns():
    """Test error message when there are no vector columns."""
    from lancedb.util import infer_vector_column_query

    schema = pa.schema({"id": pa.int64(), "text": pa.string(), "score": pa.float32()})

    with pytest.raises(ValueError) as exc_info:
        infer_vector_column_query(schema)

    error_msg = str(exc_info.value)
    assert "There is no vector column in the data" in error_msg
    assert "Please specify the vector column name for vector search" in error_msg
    assert "Available columns: ['id', 'text', 'score']" in error_msg


def test_infer_vector_column_query_multiple_vector_columns():
    """Test error message when there are multiple vector columns."""
    from lancedb.util import infer_vector_column_query

    schema = pa.schema(
        {
            "id": pa.int64(),
            "embeddings": pa.list_(pa.float32(), 384),
            "text": pa.string(),
            "image_vec": pa.list_(pa.float32(), 512),
        }
    )

    with pytest.raises(ValueError) as exc_info:
        infer_vector_column_query(schema)

    error_msg = str(exc_info.value)
    assert (
        "Schema has more than one vector column: ['embeddings', 'image_vec']"
        in error_msg
    )
    assert (
        "Please specify the vector column name for vector search using the "
        "'vector_column_name' parameter" in error_msg
    )
