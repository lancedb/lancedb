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

import functools
from pathlib import Path
from typing import List
from unittest.mock import PropertyMock, patch

import lance
import numpy as np
import pandas as pd
import pyarrow as pa
import pytest

from lancedb.conftest import MockEmbeddingFunction
from lancedb.db import LanceDBConnection
from lancedb.pydantic import LanceModel, Vector
from lancedb.table import LanceTable


class MockDB:
    def __init__(self, uri: Path):
        self.uri = uri

    @functools.cached_property
    def is_managed_remote(self) -> bool:
        return False


@pytest.fixture
def db(tmp_path) -> MockDB:
    return MockDB(tmp_path)


def test_basic(db):
    ds = LanceTable.create(
        db,
        "test",
        data=[
            {"vector": [3.1, 4.1], "item": "foo", "price": 10.0},
            {"vector": [5.9, 26.5], "item": "bar", "price": 20.0},
        ],
    ).to_lance()

    table = LanceTable(db, "test")
    assert table.name == "test"
    assert table.schema == ds.schema
    assert table.to_lance().to_table() == ds.to_table()


def test_create_table(db):
    schema = pa.schema(
        [
            pa.field("vector", pa.list_(pa.float32(), 2)),
            pa.field("item", pa.string()),
            pa.field("price", pa.float32()),
        ]
    )
    expected = pa.Table.from_arrays(
        [
            pa.FixedSizeListArray.from_arrays(pa.array([3.1, 4.1, 5.9, 26.5]), 2),
            pa.array(["foo", "bar"]),
            pa.array([10.0, 20.0]),
        ],
        schema=schema,
    )
    data = [
        [
            {"vector": [3.1, 4.1], "item": "foo", "price": 10.0},
            {"vector": [5.9, 26.5], "item": "bar", "price": 20.0},
        ]
    ]
    df = pd.DataFrame(data[0])
    data.append(df)
    data.append(pa.Table.from_pandas(df, schema=schema))

    for i, d in enumerate(data):
        tbl = (
            LanceTable.create(db, f"test_{i}", data=d, schema=schema)
            .to_lance()
            .to_table()
        )
        assert expected == tbl


def test_empty_table(db):
    schema = pa.schema(
        [
            pa.field("vector", pa.list_(pa.float32(), 2)),
            pa.field("item", pa.string()),
            pa.field("price", pa.float32()),
        ]
    )
    tbl = LanceTable.create(db, "test", schema=schema)
    data = [
        {"vector": [3.1, 4.1], "item": "foo", "price": 10.0},
        {"vector": [5.9, 26.5], "item": "bar", "price": 20.0},
    ]
    tbl.add(data=data)


def test_add(db):
    schema = pa.schema(
        [
            pa.field("vector", pa.list_(pa.float32(), 2)),
            pa.field("item", pa.string()),
            pa.field("price", pa.float64()),
        ]
    )

    table = LanceTable.create(
        db,
        "test",
        data=[
            {"vector": [3.1, 4.1], "item": "foo", "price": 10.0},
            {"vector": [5.9, 26.5], "item": "bar", "price": 20.0},
        ],
    )
    _add(table, schema)

    table = LanceTable.create(db, "test2", schema=schema)
    table.add(
        data=[
            {"vector": [3.1, 4.1], "item": "foo", "price": 10.0},
            {"vector": [5.9, 26.5], "item": "bar", "price": 20.0},
        ],
    )
    _add(table, schema)


def test_add_pydantic_model(db):
    class TestModel(LanceModel):
        vector: Vector(16)
        li: List[int]

    data = TestModel(vector=list(range(16)), li=[1, 2, 3])
    table = LanceTable.create(db, "test", data=[data])
    assert len(table) == 1
    assert table.schema == TestModel.to_arrow_schema()


def _add(table, schema):
    # table = LanceTable(db, "test")
    assert len(table) == 2

    table.add([{"vector": [6.3, 100.5], "item": "new", "price": 30.0}])
    assert len(table) == 3

    expected = pa.Table.from_arrays(
        [
            pa.FixedSizeListArray.from_arrays(
                pa.array([3.1, 4.1, 5.9, 26.5, 6.3, 100.5]), 2
            ),
            pa.array(["foo", "bar", "new"]),
            pa.array([10.0, 20.0, 30.0]),
        ],
        schema=schema,
    )
    assert expected == table.to_arrow()


def test_versioning(db):
    table = LanceTable.create(
        db,
        "test",
        data=[
            {"vector": [3.1, 4.1], "item": "foo", "price": 10.0},
            {"vector": [5.9, 26.5], "item": "bar", "price": 20.0},
        ],
    )

    assert len(table.list_versions()) == 2
    assert table.version == 2

    table.add([{"vector": [6.3, 100.5], "item": "new", "price": 30.0}])
    assert len(table.list_versions()) == 3
    assert table.version == 3
    assert len(table) == 3

    table.checkout(2)
    assert table.version == 2
    assert len(table) == 2


def test_create_index_method():
    with patch.object(LanceTable, "_reset_dataset", return_value=None):
        with patch.object(
            LanceTable, "_dataset", new_callable=PropertyMock
        ) as mock_dataset:
            # Setup mock responses
            mock_dataset.return_value.create_index.return_value = None

            # Create a LanceTable object
            connection = LanceDBConnection(uri="mock.uri")
            table = LanceTable(connection, "test_table")

            # Call the create_index method
            table.create_index(
                metric="L2",
                num_partitions=256,
                num_sub_vectors=96,
                vector_column_name="vector",
                replace=True,
            )

            # Check that the _dataset.create_index method was called
            # with the right parameters
            mock_dataset.return_value.create_index.assert_called_once_with(
                column="vector",
                index_type="IVF_PQ",
                metric="L2",
                num_partitions=256,
                num_sub_vectors=96,
                replace=True,
            )


def test_add_with_nans(db):
    # by default we raise an error on bad input vectors
    bad_data = [
        {"vector": [np.nan], "item": "bar", "price": 20.0},
        {"vector": [5], "item": "bar", "price": 20.0},
        {"vector": [np.nan, np.nan], "item": "bar", "price": 20.0},
        {"vector": [np.nan, 5.0], "item": "bar", "price": 20.0},
    ]
    for row in bad_data:
        with pytest.raises(ValueError):
            LanceTable.create(
                db,
                "error_test",
                data=[{"vector": [3.1, 4.1], "item": "foo", "price": 10.0}, row],
            )

    table = LanceTable.create(
        db,
        "drop_test",
        data=[
            {"vector": [3.1, 4.1], "item": "foo", "price": 10.0},
            {"vector": [np.nan], "item": "bar", "price": 20.0},
            {"vector": [5], "item": "bar", "price": 20.0},
            {"vector": [np.nan, np.nan], "item": "bar", "price": 20.0},
        ],
        on_bad_vectors="drop",
    )
    assert len(table) == 1

    # We can fill bad input with some value
    table = LanceTable.create(
        db,
        "fill_test",
        data=[
            {"vector": [3.1, 4.1], "item": "foo", "price": 10.0},
            {"vector": [np.nan], "item": "bar", "price": 20.0},
            {"vector": [np.nan, np.nan], "item": "bar", "price": 20.0},
        ],
        on_bad_vectors="fill",
        fill_value=0.0,
    )
    assert len(table) == 3
    arrow_tbl = table.to_lance().to_table(filter="item == 'bar'")
    v = arrow_tbl["vector"].to_pylist()[0]
    assert np.allclose(v, np.array([0.0, 0.0]))


def test_restore(db):
    table = LanceTable.create(
        db,
        "my_table",
        data=[{"vector": [1.1, 0.9], "type": "vector"}],
    )
    table.add([{"vector": [0.5, 0.2], "type": "vector"}])
    table.restore(2)
    assert len(table.list_versions()) == 4
    assert len(table) == 1

    expected = table.to_arrow()
    table.checkout(2)
    table.restore()
    assert len(table.list_versions()) == 5
    assert table.to_arrow() == expected

    table.restore(5)  # latest version should be no-op
    assert len(table.list_versions()) == 5

    with pytest.raises(ValueError):
        table.restore(6)

    with pytest.raises(ValueError):
        table.restore(0)


def test_merge(db, tmp_path):
    table = LanceTable.create(
        db,
        "my_table",
        data=[{"vector": [1.1, 0.9], "id": 0}, {"vector": [1.2, 1.9], "id": 1}],
    )
    other_table = pa.table({"document": ["foo", "bar"], "id": [0, 1]})
    table.merge(other_table, left_on="id")
    assert len(table.list_versions()) == 3
    expected = pa.table(
        {"vector": [[1.1, 0.9], [1.2, 1.9]], "id": [0, 1], "document": ["foo", "bar"]},
        schema=table.schema,
    )
    assert table.to_arrow() == expected

    other_dataset = lance.write_dataset(other_table, tmp_path / "other_table.lance")
    table.restore(1)
    table.merge(other_dataset, left_on="id")


def test_delete(db):
    table = LanceTable.create(
        db,
        "my_table",
        data=[{"vector": [1.1, 0.9], "id": 0}, {"vector": [1.2, 1.9], "id": 1}],
    )
    assert len(table) == 2
    assert len(table.list_versions()) == 2
    table.delete("id=0")
    assert len(table.list_versions()) == 3
    assert table.version == 3
    assert len(table) == 1
    assert table.to_pandas()["id"].tolist() == [1]


def test_update(db):
    table = LanceTable.create(
        db,
        "my_table",
        data=[{"vector": [1.1, 0.9], "id": 0}, {"vector": [1.2, 1.9], "id": 1}],
    )
    assert len(table) == 2
    assert len(table.list_versions()) == 2
    table.update(where="id=0", values={"vector": [1.1, 1.1]})
    assert len(table.list_versions()) == 4
    assert table.version == 4
    assert len(table) == 2
    v = table.to_arrow()["vector"].combine_chunks()
    v = v.values.to_numpy().reshape(2, 2)
    assert np.allclose(v, np.array([[1.2, 1.9], [1.1, 1.1]]))


def test_create_with_embedding_function(db):
    class MyTable(LanceModel):
        text: str
        vector: Vector(10)

    func = MockEmbeddingFunction(source_column="text", vector_column="vector")
    texts = ["hello world", "goodbye world", "foo bar baz fizz buzz"]
    df = pd.DataFrame({"text": texts, "vector": func(texts)})

    table = LanceTable.create(
        db,
        "my_table",
        schema=MyTable,
        embedding_functions=[func],
    )
    table.add(df)

    query_str = "hi how are you?"
    query_vector = func(query_str)[0]
    expected = table.search(query_vector).limit(2).to_arrow()

    actual = table.search(query_str).limit(2).to_arrow()
    assert actual == expected


def test_add_with_embedding_function(db):
    class MyTable(LanceModel):
        text: str
        vector: Vector(10)

    func = MockEmbeddingFunction(source_column="text", vector_column="vector")
    table = LanceTable.create(
        db,
        "my_table",
        schema=MyTable,
        embedding_functions=[func],
    )

    texts = ["hello world", "goodbye world", "foo bar baz fizz buzz"]
    df = pd.DataFrame({"text": texts})
    table.add(df)

    texts = ["the quick brown fox", "jumped over the lazy dog"]
    table.add([{"text": t} for t in texts])

    query_str = "hi how are you?"
    query_vector = func(query_str)[0]
    expected = table.search(query_vector).limit(2).to_arrow()

    actual = table.search(query_str).limit(2).to_arrow()
    assert actual == expected


def test_multiple_vector_columns(db):
    class MyTable(LanceModel):
        text: str
        vector1: Vector(10)
        vector2: Vector(10)

    table = LanceTable.create(
        db,
        "my_table",
        schema=MyTable,
    )

    v1 = np.random.randn(10)
    v2 = np.random.randn(10)
    data = [
        {"vector1": v1, "vector2": v2, "text": "foo"},
        {"vector1": v2, "vector2": v1, "text": "bar"},
    ]
    df = pd.DataFrame(data)
    table.add(df)

    q = np.random.randn(10)
    result1 = table.search(q, vector_column_name="vector1").limit(1).to_df()
    result2 = table.search(q, vector_column_name="vector2").limit(1).to_df()

    assert result1["text"].iloc[0] != result2["text"].iloc[0]


def test_empty_query(db):
    table = LanceTable.create(
        db,
        "my_table",
        data=[{"text": "foo", "id": 0}, {"text": "bar", "id": 1}],
    )
    df = table.search().select(["id"]).where("text='bar'").limit(1).to_df()
    val = df.id.iloc[0]
    assert val == 1
