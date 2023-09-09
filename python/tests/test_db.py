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

import numpy as np
import pandas as pd
import pyarrow as pa
import pytest

import lancedb
from lancedb.pydantic import LanceModel, Vector


def test_basic(tmp_path):
    db = lancedb.connect(tmp_path)

    assert db.uri == str(tmp_path)
    assert db.table_names() == []

    table = db.create_table(
        "test",
        data=[
            {"vector": [3.1, 4.1], "item": "foo", "price": 10.0},
            {"vector": [5.9, 26.5], "item": "bar", "price": 20.0},
        ],
    )
    rs = table.search([100, 100]).limit(1).to_df()
    assert len(rs) == 1
    assert rs["item"].iloc[0] == "bar"

    rs = table.search([100, 100]).where("price < 15").limit(2).to_df()
    assert len(rs) == 1
    assert rs["item"].iloc[0] == "foo"

    assert db.table_names() == ["test"]
    assert "test" in db
    assert len(db) == 1

    assert db.open_table("test").name == db["test"].name


def test_ingest_pd(tmp_path):
    db = lancedb.connect(tmp_path)

    assert db.uri == str(tmp_path)
    assert db.table_names() == []

    data = pd.DataFrame(
        {
            "vector": [[3.1, 4.1], [5.9, 26.5]],
            "item": ["foo", "bar"],
            "price": [10.0, 20.0],
        }
    )
    table = db.create_table("test", data=data)
    rs = table.search([100, 100]).limit(1).to_df()
    assert len(rs) == 1
    assert rs["item"].iloc[0] == "bar"

    rs = table.search([100, 100]).where("price < 15").limit(2).to_df()
    assert len(rs) == 1
    assert rs["item"].iloc[0] == "foo"

    assert db.table_names() == ["test"]
    assert "test" in db
    assert len(db) == 1

    assert db.open_table("test").name == db["test"].name


def test_ingest_iterator(tmp_path):
    class PydanticSchema(LanceModel):
        vector: Vector(2)
        item: str
        price: float

    arrow_schema = pa.schema(
        [
            pa.field("vector", pa.list_(pa.float32(), 2)),
            pa.field("item", pa.utf8()),
            pa.field("price", pa.float32()),
        ]
    )

    def make_batches():
        for _ in range(5):
            yield from [
                # pandas
                pd.DataFrame(
                    {
                        "vector": [[3.1, 4.1], [1, 1]],
                        "item": ["foo", "bar"],
                        "price": [10.0, 20.0],
                    }
                ),
                # pylist
                [
                    {"vector": [3.1, 4.1], "item": "foo", "price": 10.0},
                    {"vector": [5.9, 26.5], "item": "bar", "price": 20.0},
                ],
                # recordbatch
                pa.RecordBatch.from_arrays(
                    [
                        pa.array([[3.1, 4.1], [5.9, 26.5]], pa.list_(pa.float32(), 2)),
                        pa.array(["foo", "bar"]),
                        pa.array([10.0, 20.0]),
                    ],
                    ["vector", "item", "price"],
                ),
                # pa Table
                pa.Table.from_arrays(
                    [
                        pa.array([[3.1, 4.1], [5.9, 26.5]], pa.list_(pa.float32(), 2)),
                        pa.array(["foo", "bar"]),
                        pa.array([10.0, 20.0]),
                    ],
                    ["vector", "item", "price"],
                ),
                # pydantic list
                [
                    PydanticSchema(vector=[3.1, 4.1], item="foo", price=10.0),
                    PydanticSchema(vector=[5.9, 26.5], item="bar", price=20.0),
                ]
                # TODO: test pydict separately. it is unique column number and names contraint
            ]

    def run_tests(schema):
        db = lancedb.connect(tmp_path)
        tbl = db.create_table("table2", make_batches(), schema=schema, mode="overwrite")

        tbl.to_pandas()
        assert tbl.search([3.1, 4.1]).limit(1).to_df()["_distance"][0] == 0.0
        assert tbl.search([5.9, 26.5]).limit(1).to_df()["_distance"][0] == 0.0

        tbl_len = len(tbl)
        tbl.add(make_batches())
        assert tbl_len == 50
        assert len(tbl) == tbl_len * 2
        assert len(tbl.list_versions()) == 3
        db.drop_database()

    run_tests(arrow_schema)
    run_tests(PydanticSchema)


def test_create_mode(tmp_path):
    db = lancedb.connect(tmp_path)
    data = pd.DataFrame(
        {
            "vector": [[3.1, 4.1], [5.9, 26.5]],
            "item": ["foo", "bar"],
            "price": [10.0, 20.0],
        }
    )
    db.create_table("test", data=data)

    with pytest.raises(Exception):
        db.create_table("test", data=data)

    new_data = pd.DataFrame(
        {
            "vector": [[3.1, 4.1], [5.9, 26.5]],
            "item": ["fizz", "buzz"],
            "price": [10.0, 20.0],
        }
    )
    tbl = db.create_table("test", data=new_data, mode="overwrite")
    assert tbl.to_pandas().item.tolist() == ["fizz", "buzz"]


def test_delete_table(tmp_path):
    db = lancedb.connect(tmp_path)
    data = pd.DataFrame(
        {
            "vector": [[3.1, 4.1], [5.9, 26.5]],
            "item": ["foo", "bar"],
            "price": [10.0, 20.0],
        }
    )
    db.create_table("test", data=data)

    with pytest.raises(Exception):
        db.create_table("test", data=data)

    assert db.table_names() == ["test"]

    db.drop_table("test")
    assert db.table_names() == []

    db.create_table("test", data=data)
    assert db.table_names() == ["test"]

    # dropping a table that does not exist should pass
    # if ignore_missing=True
    db.drop_table("does_not_exist", ignore_missing=True)


def test_drop_database(tmp_path):
    db = lancedb.connect(tmp_path)
    data = pd.DataFrame(
        {
            "vector": [[3.1, 4.1], [5.9, 26.5]],
            "item": ["foo", "bar"],
            "price": [10.0, 20.0],
        }
    )
    new_data = pd.DataFrame(
        {
            "vector": [[5.1, 4.1], [5.9, 10.5]],
            "item": ["kiwi", "avocado"],
            "price": [12.0, 17.0],
        }
    )
    db.create_table("test", data=data)
    with pytest.raises(Exception):
        db.create_table("test", data=data)

    assert db.table_names() == ["test"]

    db.create_table("new_test", data=new_data)
    db.drop_database()
    assert db.table_names() == []

    # it should pass when no tables are present
    db.create_table("test", data=new_data)
    db.drop_table("test")
    assert db.table_names() == []
    db.drop_database()
    assert db.table_names() == []

    # creating an empty database with schema
    schema = pa.schema([pa.field("vector", pa.list_(pa.float32(), list_size=2))])
    db.create_table("empty_table", schema=schema)
    # dropping a empty database should pass
    db.drop_database()
    assert db.table_names() == []


def test_empty_or_nonexistent_table(tmp_path):
    db = lancedb.connect(tmp_path)
    with pytest.raises(Exception):
        db.create_table("test_with_no_data")

    with pytest.raises(Exception):
        db.open_table("does_not_exist")

    schema = pa.schema([pa.field("a", pa.int64(), nullable=False)])
    test = db.create_table("test", schema=schema)

    class TestModel(LanceModel):
        a: int

    test2 = db.create_table("test2", schema=TestModel)
    assert test.schema == test2.schema


def test_replace_index(tmp_path):
    db = lancedb.connect(uri=tmp_path)
    table = db.create_table(
        "test",
        [
            {"vector": np.random.rand(128), "item": "foo", "price": float(i)}
            for i in range(1000)
        ],
    )
    table.create_index(
        num_partitions=2,
        num_sub_vectors=4,
    )

    with pytest.raises(Exception):
        table.create_index(
            num_partitions=2,
            num_sub_vectors=4,
            replace=False,
        )

    table.create_index(
        num_partitions=2,
        num_sub_vectors=4,
        replace=True,
    )
