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
from unittest.mock import PropertyMock, patch

import numpy as np
import pandas as pd
import pyarrow as pa
import pytest
from lance.vector import vec_to_table

from lancedb.db import LanceDBConnection
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

    assert len(table.list_versions()) == 1
    assert table.version == 1

    table.add([{"vector": [6.3, 100.5], "item": "new", "price": 30.0}])
    assert len(table.list_versions()) == 2
    assert table.version == 2
    assert len(table) == 3

    table.checkout(1)
    assert table.version == 1
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
