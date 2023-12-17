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
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import List
from unittest.mock import PropertyMock, patch

import lance
import numpy as np
import pandas as pd
import pyarrow as pa
from pydantic import BaseModel
import pytest

from lancedb.conftest import MockTextEmbeddingFunction
from lancedb.db import LanceDBConnection
from lancedb.embeddings import EmbeddingFunctionConfig, EmbeddingFunctionRegistry
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
    # https://github.com/lancedb/lancedb/issues/562

    class Document(BaseModel):
        content: str
        source: str

    class LanceSchema(LanceModel):
        id: str
        vector: Vector(2)
        li: List[int]
        payload: Document

    tbl = LanceTable.create(db, "mytable", schema=LanceSchema, mode="overwrite")
    assert tbl.schema == LanceSchema.to_arrow_schema()

    # add works
    expected = LanceSchema(
        id="id",
        vector=[0.0, 0.0],
        li=[1, 2, 3],
        payload=Document(content="foo", source="bar"),
    )
    tbl.add([expected])

    result = tbl.search([0.0, 0.0]).limit(1).to_pydantic(LanceSchema)[0]
    assert result == expected


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
                index_cache_size=256,
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
                accelerator=None,
                index_cache_size=256,
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
    assert len(table.list_versions()) == 3
    assert table.version == 3
    assert len(table) == 2
    v = table.to_arrow()["vector"].combine_chunks()
    v = v.values.to_numpy().reshape(2, 2)
    assert np.allclose(v, np.array([[1.2, 1.9], [1.1, 1.1]]))


def test_update_types(db):
    table = LanceTable.create(
        db,
        "my_table",
        data=[
            {
                "id": 0,
                "str": "foo",
                "float": 1.1,
                "timestamp": datetime(2021, 1, 1),
                "date": date(2021, 1, 1),
                "vector1": [1.0, 0.0],
                "vector2": [1.0, 1.0],
            }
        ],
    )
    # Update with SQL
    table.update(
        values_sql=dict(
            id="1",
            str="'bar'",
            float="2.2",
            timestamp="TIMESTAMP '2021-01-02 00:00:00'",
            date="DATE '2021-01-02'",
            vector1="[2.0, 2.0]",
            vector2="[3.0, 3.0]",
        )
    )
    actual = table.to_arrow().to_pylist()[0]
    expected = dict(
        id=1,
        str="bar",
        float=2.2,
        timestamp=datetime(2021, 1, 2),
        date=date(2021, 1, 2),
        vector1=[2.0, 2.0],
        vector2=[3.0, 3.0],
    )
    assert actual == expected

    # Update with values
    table.update(
        values=dict(
            id=2,
            str="baz",
            float=3.3,
            timestamp=datetime(2021, 1, 3),
            date=date(2021, 1, 3),
            vector1=[3.0, 3.0],
            vector2=np.array([4.0, 4.0]),
        )
    )
    actual = table.to_arrow().to_pylist()[0]
    expected = dict(
        id=2,
        str="baz",
        float=3.3,
        timestamp=datetime(2021, 1, 3),
        date=date(2021, 1, 3),
        vector1=[3.0, 3.0],
        vector2=[4.0, 4.0],
    )
    assert actual == expected


def test_create_with_embedding_function(db):
    class MyTable(LanceModel):
        text: str
        vector: Vector(10)

    func = MockTextEmbeddingFunction()
    texts = ["hello world", "goodbye world", "foo bar baz fizz buzz"]
    df = pd.DataFrame({"text": texts, "vector": func.compute_source_embeddings(texts)})

    conf = EmbeddingFunctionConfig(
        source_column="text", vector_column="vector", function=func
    )
    table = LanceTable.create(
        db,
        "my_table",
        schema=MyTable,
        embedding_functions=[conf],
    )
    table.add(df)

    query_str = "hi how are you?"
    query_vector = func.compute_query_embeddings(query_str)[0]
    expected = table.search(query_vector).limit(2).to_arrow()

    actual = table.search(query_str).limit(2).to_arrow()
    assert actual == expected


def test_add_with_embedding_function(db):
    emb = EmbeddingFunctionRegistry.get_instance().get("test")()

    class MyTable(LanceModel):
        text: str = emb.SourceField()
        vector: Vector(emb.ndims()) = emb.VectorField()

    table = LanceTable.create(db, "my_table", schema=MyTable)

    texts = ["hello world", "goodbye world", "foo bar baz fizz buzz"]
    df = pd.DataFrame({"text": texts})
    table.add(df)

    texts = ["the quick brown fox", "jumped over the lazy dog"]
    table.add([{"text": t} for t in texts])

    query_str = "hi how are you?"
    query_vector = emb.compute_query_embeddings(query_str)[0]
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
    result1 = table.search(q, vector_column_name="vector1").limit(1).to_pandas()
    result2 = table.search(q, vector_column_name="vector2").limit(1).to_pandas()

    assert result1["text"].iloc[0] != result2["text"].iloc[0]


def test_empty_query(db):
    table = LanceTable.create(
        db,
        "my_table",
        data=[{"text": "foo", "id": 0}, {"text": "bar", "id": 1}],
    )
    df = table.search().select(["id"]).where("text='bar'").limit(1).to_pandas()
    val = df.id.iloc[0]
    assert val == 1


def test_compact_cleanup(db):
    table = LanceTable.create(
        db,
        "my_table",
        data=[{"text": "foo", "id": 0}, {"text": "bar", "id": 1}],
    )

    table.add([{"text": "baz", "id": 2}])
    assert len(table) == 3
    assert table.version == 3

    stats = table.compact_files()
    assert len(table) == 3
    # Compact_files bump 2 versions.
    assert table.version == 5
    assert stats.fragments_removed > 0
    assert stats.fragments_added == 1

    stats = table.cleanup_old_versions()
    assert stats.bytes_removed == 0

    stats = table.cleanup_old_versions(older_than=timedelta(0), delete_unverified=True)
    assert stats.bytes_removed > 0
    assert table.version == 5

    with pytest.raises(Exception, match="Version 3 no longer exists"):
        table.checkout(3)


def test_hybrid_search(db):
    # Create a LanceDB table schema with a vector and a text column
    class MyTable(LanceModel):
        text: str
        vector: Vector(2)

    # Initialize the table using the schema
    table = LanceTable.create(
        db,
        "my_table",
        schema=MyTable,
    )

    # Create a list of 10 unique english phrases
    phrases = [
        "great kid don't get cocky",
        "now that's a name I haven't heard in a long time",
        "if you strike me down I shall become more powerful than you can possibly imagine",
        "I find your lack of faith disturbing",
        "I've got a bad feeling about this",
        "never tell me the odds",
        "I am your father",
        "somebody has to save our skins",
        "New strategy R2 let the wookiee win",
        "Arrrrggghhhhhhh",
    ]

    # Create 10 2-dimensional vectors evenly spaced around the unit circle
    vectors = [
        [np.cos(2 * np.pi * i / 10), np.sin(2 * np.pi * i / 10)] for i in range(10)
    ]

    # Add the phrases and vectors to the table
    table.add(
        [{"text": phrase, "vector": vector} for phrase, vector in zip(phrases, vectors)]
    )

    # Create a fts index
    table.create_fts_index("text")

    result1 = (
        table.search("Our father who art in heaven", type="hybrid")
        .rerank(weight=0.5, normalize="auto")
        .limit(10)
        .to_pydantic(MyTable)
    )
    result2 = (
        table.search("Our father who art in heaven", type="hybrid")
        .rerank(weight=0.5)
        .limit(10)
        .to_pydantic(MyTable)
    )
    assert result1 == result2

    result = (
        table.search("Our father who art in heaven", type="hybrid")
        .rerank(weight=0.5, normalize="score")
        .limit(10)
        .to_pydantic(MyTable)
    )
    assert 0 == 1
