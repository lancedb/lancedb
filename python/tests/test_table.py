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

from pathlib import Path

import pandas as pd
import pyarrow as pa
import pytest

from lancedb.table import LanceTable


class MockDB:
    def __init__(self, uri: Path):
        self.uri = uri


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


def test_add(db):
    table = LanceTable.create(
        db,
        "test",
        data=[
            {"vector": [3.1, 4.1], "item": "foo", "price": 10.0},
            {"vector": [5.9, 26.5], "item": "bar", "price": 20.0},
        ],
    )

    # table = LanceTable(db, "test")
    assert len(table) == 2

    count = table.add([{"vector": [6.3, 100.5], "item": "new", "price": 30.0}])
    assert count == 3

    expected = pa.Table.from_arrays(
        [
            pa.FixedSizeListArray.from_arrays(
                pa.array([3.1, 4.1, 5.9, 26.5, 6.3, 100.5]), 2
            ),
            pa.array(["foo", "bar", "new"]),
            pa.array([10.0, 20.0, 30.0]),
        ],
        schema=pa.schema(
            [
                pa.field("vector", pa.list_(pa.float32(), 2)),
                pa.field("item", pa.string()),
                pa.field("price", pa.float64()),
            ]
        ),
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

    assert len(table.list_versions()) == 1
    assert table.version == 1

    table.add([{"vector": [6.3, 100.5], "item": "new", "price": 30.0}])
    assert len(table.list_versions()) == 2
    assert table.version == 2
    assert len(table) == 3

    table.checkout(1)
    assert table.version == 1
    assert len(table) == 2
