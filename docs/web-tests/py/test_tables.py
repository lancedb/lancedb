# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

# --8<-- [start:tables_imports]
import lancedb
import numpy as np
import pandas as pd
import pyarrow as pa
import pytest
from numpy.random import randint, random

# --8<-- [end:tables_imports]


# ============================================================================
# Table Creation Examples
# ============================================================================


def test_tables_basic_connect_snippet(monkeypatch):
    calls = {}

    class DummyDB:
        pass

    def fake_connect(uri):
        calls["uri"] = uri
        return DummyDB()

    import lancedb as _lancedb

    # Monkey patch is used only in this test.
    # Maybe can just actually connect, and disconnect instead?
    monkeypatch.setattr(_lancedb, "connect", fake_connect)

    # --8<-- [start:tables_basic_connect]
    import lancedb

    uri = "data/sample-lancedb"
    db = lancedb.connect(uri)
    # --8<-- [end:tables_basic_connect]

    assert calls["uri"] == "data/sample-lancedb"
    assert isinstance(db, DummyDB)


def test_update_connect_enterprise_snippet(monkeypatch):
    calls = {}

    class DummyDB:
        pass

    def fake_connect(**kwargs):
        calls.update(kwargs)
        return DummyDB()

    import lancedb as _lancedb

    monkeypatch.setattr(_lancedb, "connect", fake_connect)

    # --8<-- [start:update_connect_enterprise]
    import lancedb

    db = lancedb.connect(
        uri="db://your-project-slug",
        api_key="your-api-key",
        region="us-east-1",
    )
    # --8<-- [end:update_connect_enterprise]

    assert calls["uri"] == "db://your-project-slug"
    assert calls["api_key"] == "your-api-key"
    assert calls["region"] == "us-east-1"
    assert isinstance(db, DummyDB)


def test_update_connect_local_snippet(monkeypatch):
    calls = {}

    class DummyDB:
        pass

    def fake_connect(uri):
        calls["uri"] = uri
        return DummyDB()

    import lancedb as _lancedb

    monkeypatch.setattr(_lancedb, "connect", fake_connect)

    # --8<-- [start:update_connect_local]
    import lancedb

    db = lancedb.connect("./data")
    # --8<-- [end:update_connect_local]

    assert calls["uri"] == "./data"
    assert isinstance(db, DummyDB)


def test_table_creation_from_dicts(tmp_db):
    # --8<-- [start:create_table_from_dicts]
    data = [
        {"vector": [1.1, 1.2], "lat": 45.5, "long": -122.7},
        {"vector": [0.2, 1.8], "lat": 40.1, "long": -74.1},
    ]
    db = tmp_db
    db.create_table("test_table", data, mode="overwrite")
    tbl = db["test_table"]
    tbl.head()
    # --8<-- [end:create_table_from_dicts]


def test_create_table_conflict_handling(tmp_db):
    db = tmp_db
    data = [
        {"vector": [1.1, 1.2], "lat": 45.5, "long": -122.7},
        {"vector": [0.2, 1.8], "lat": 40.1, "long": -74.1},
    ]
    db.create_table("conflict_table", data)

    # --8<-- [start:create_table_conflict_handling]
    # Idempotent open: reuse the existing table if it exists.
    # The provided data is ignored; the schema is validated against the
    # existing table and a mismatch raises an error.
    tbl = db.create_table("conflict_table", data, exist_ok=True)

    # Overwrite: drop the existing table and create a new one with the
    # provided data. This permanently discards the old table's data.
    tbl = db.create_table("conflict_table", data, mode="overwrite")
    # --8<-- [end:create_table_conflict_handling]
    assert tbl.count_rows() == 2


def test_table_creation_from_pandas(tmp_db):
    # --8<-- [start:create_table_from_pandas]
    import pandas as pd

    data = pd.DataFrame(
        {
            "vector": [[1.1, 1.2, 1.3, 1.4], [0.2, 1.8, 0.4, 3.6]],
            "lat": [45.5, 40.1],
            "long": [-122.7, -74.1],
        }
    )
    db = tmp_db
    db.create_table("my_table_pandas", data, mode="overwrite")
    db["my_table_pandas"].head()
    # --8<-- [end:create_table_from_pandas]


def test_table_creation_with_custom_schema(tmp_db):
    # --8<-- [start:create_table_custom_schema]
    import pyarrow as pa

    custom_schema = pa.schema(
        [
            pa.field("vector", pa.list_(pa.float32(), 4)),
            pa.field("lat", pa.float32()),
            pa.field("long", pa.float32()),
        ]
    )

    data = [
        {"vector": [1.1, 1.2, 1.3, 1.4], "lat": 45.5, "long": -122.7},
        {"vector": [0.2, 1.8, 0.4, 3.6], "lat": 40.1, "long": -74.1},
    ]
    db = tmp_db
    tbl = db.create_table(
        "my_table_custom_schema", data, schema=custom_schema, mode="overwrite"
    )
    # --8<-- [end:create_table_custom_schema]


def test_table_creation_from_polars(tmp_db):
    # --8<-- [start:create_table_from_polars]
    import polars as pl

    data = pl.DataFrame(
        {
            "vector": [[3.1, 4.1], [5.9, 26.5]],
            "item": ["foo", "bar"],
            "price": [10.0, 20.0],
        }
    )
    db = tmp_db
    tbl = db.create_table("my_table_pl", data, mode="overwrite")
    # --8<-- [end:create_table_from_polars]


def test_table_creation_from_arrow(tmp_db):
    # --8<-- [start:create_table_from_arrow]
    import numpy as np
    import pyarrow as pa

    dim = 16
    total = 2
    schema = pa.schema(
        [pa.field("vector", pa.list_(pa.float16(), dim)), pa.field("text", pa.string())]
    )
    data = pa.Table.from_arrays(
        [
            pa.array(
                [np.random.randn(dim).astype(np.float16) for _ in range(total)],
                pa.list_(pa.float16(), dim),
            ),
            pa.array(["foo", "bar"]),
        ],
        ["vector", "text"],
    )
    db = tmp_db
    tbl = db.create_table("f16_tbl", data, schema=schema, mode="overwrite")
    # --8<-- [end:create_table_from_arrow]


def test_table_creation_from_pydantic(tmp_db):
    # --8<-- [start:create_table_from_pydantic]
    from lancedb.pydantic import LanceModel, Vector

    class Content(LanceModel):
        movie_id: int
        vector: Vector(128)
        genres: str
        title: str
        imdb_id: int

        @property
        def imdb_url(self) -> str:
            return f"https://www.imdb.com/title/tt{self.imdb_id}"

    db = tmp_db
    tbl = db.create_table("movielens_small", schema=Content, mode="overwrite")
    # --8<-- [end:create_table_from_pydantic]


def test_table_creation_nested_schema(tmp_db):
    # --8<-- [start:create_table_nested_schema]
    from lancedb.pydantic import LanceModel, Vector

    # --8<-- [start:tables_document_model]
    from pydantic import BaseModel

    class Document(BaseModel):
        content: str
        source: str

    # --8<-- [end:tables_document_model]

    class NestedSchema(LanceModel):
        id: str
        vector: Vector(1536)
        document: Document

    db = tmp_db
    tbl = db.create_table("nested_table", schema=NestedSchema, mode="overwrite")
    # --8<-- [end:create_table_nested_schema]


def test_tables_tz_validator_snippet():
    # --8<-- [start:tables_tz_validator]
    from datetime import datetime
    from zoneinfo import ZoneInfo

    from lancedb.pydantic import LanceModel
    from pydantic import Field, ValidationError, ValidationInfo, field_validator

    tzname = "America/New_York"
    tz = ZoneInfo(tzname)

    class TestModel(LanceModel):
        dt_with_tz: datetime = Field(json_schema_extra={"tz": tzname})

        @field_validator("dt_with_tz")
        @classmethod
        def tz_must_match(cls, dt: datetime) -> datetime:
            assert dt.tzinfo == tz
            return dt

    ok = TestModel(dt_with_tz=datetime.now(tz))

    try:
        TestModel(dt_with_tz=datetime.now(ZoneInfo("Asia/Shanghai")))
        assert 0 == 1, "this should raise ValidationError"
    except ValidationError:
        print("A ValidationError was raised.")
        pass
    # --8<-- [end:tables_tz_validator]

    assert ok is not None


def test_add_from_dataset(tmp_db, tmp_path):
    import pyarrow as pa
    import pyarrow.dataset as ds
    import pyarrow.parquet as pq

    schema = pa.schema(
        [
            pa.field("vector", pa.list_(pa.float32(), 4)),
            pa.field("item", pa.utf8()),
            pa.field("price", pa.float32()),
        ]
    )
    for i in range(3):
        batch = pa.table(
            {
                "vector": [[1.0, 2.0, 3.0, 4.0], [5.0, 6.0, 7.0, 8.0]],
                "item": [f"item{i * 2}", f"item{i * 2 + 1}"],
                "price": [float(i * 2), float(i * 2 + 1)],
            },
            schema=schema,
        )
        pq.write_table(batch, tmp_path / f"part-{i}.parquet")

    data_path = str(tmp_path)

    # --8<-- [start:add_from_dataset]
    import pyarrow.dataset as ds

    dataset = ds.dataset(data_path, format="parquet")
    db = tmp_db
    table = db.create_table("my_table", schema=dataset.schema, mode="overwrite")
    table.add(dataset)
    # --8<-- [end:add_from_dataset]
    assert table.count_rows() == 6


def test_table_creation_from_iterator(tmp_db):
    # --8<-- [start:create_table_from_iterator]
    import pyarrow as pa

    schema = pa.schema(
        [
            pa.field("vector", pa.list_(pa.float32(), 4)),
            pa.field("item", pa.utf8()),
            pa.field("price", pa.float32()),
        ]
    )

    def make_batches():
        for i in range(5):
            yield pa.RecordBatch.from_arrays(
                [
                    pa.array(
                        [[3.1, 4.1, 5.1, 6.1], [5.9, 26.5, 4.7, 32.8]],
                        pa.list_(pa.float32(), 4),
                    ),
                    pa.array(["foo", "bar"]),
                    pa.array([10.0, 20.0]),
                ],
                ["vector", "item", "price"],
            )

    db = tmp_db
    db.create_table("batched_table", make_batches(), schema=schema, mode="overwrite")
    # --8<-- [end:create_table_from_iterator]

def test_open_existing_table(tmp_db):
    # --8<-- [start:open_existing_table]
    db = tmp_db
    # Create a table first
    data = [{"vector": [1.1, 1.2], "lat": 45.5, "long": -122.7}]
    db.create_table("test_table", data, mode="overwrite")

    # List table names
    print(db.list_tables().tables)

    # Open existing table
    tbl = db.open_table("test_table")
    # --8<-- [end:open_existing_table]


def test_create_empty_table(tmp_db):
    # --8<-- [start:create_empty_table]
    import pyarrow as pa

    schema = pa.schema(
        [
            pa.field("vector", pa.list_(pa.float32(), 2)),
            pa.field("item", pa.string()),
            pa.field("price", pa.float32()),
        ]
    )
    db = tmp_db
    tbl = db.create_table("test_empty_table", schema=schema, mode="overwrite")
    # --8<-- [end:create_empty_table]


def test_create_empty_table_pydantic(tmp_db):
    # --8<-- [start:create_empty_table_pydantic]
    from lancedb.pydantic import LanceModel, Vector

    class Item(LanceModel):
        vector: Vector(2)
        item: str
        price: float

    db = tmp_db
    tbl = db.create_table(
        "test_empty_table_new", schema=Item.to_arrow_schema(), mode="overwrite"
    )
    # --8<-- [end:create_empty_table_pydantic]


def test_drop_table(tmp_db):
    # --8<-- [start:drop_table]
    db = tmp_db
    # Create a table first
    data = [{"vector": [1.1, 1.2], "lat": 45.5}]
    db.create_table("my_table", data, mode="overwrite")

    # Drop the table
    db.drop_table("my_table")
    # --8<-- [end:drop_table]


# ============================================================================
# Data Update Examples
# ============================================================================


def test_add_data_to_table(tmp_db):
    db = tmp_db

    # --8<-- [start:add_data_to_table]
    import pyarrow as pa

    # create an empty table with schema
    data = [
        {"vector": [3.1, 4.1], "item": "foo", "price": 10.0},
        {"vector": [5.9, 26.5], "item": "bar", "price": 20.0},
        {"vector": [10.2, 100.8], "item": "baz", "price": 30.0},
        {"vector": [1.4, 9.5], "item": "fred", "price": 40.0},
    ]

    schema = pa.schema(
        [
            pa.field("vector", pa.list_(pa.float32(), 2)),
            pa.field("item", pa.utf8()),
            pa.field("price", pa.float32()),
        ]
    )

    table_name = "basic_ingestion_example"
    table = db.create_table(table_name, schema=schema, mode="overwrite")
    # Add data
    table.add(data)
    # --8<-- [end:add_data_to_table]
    assert table.count_rows() == len(data)


def test_add_data_pydantic_model(tmp_db):
    db = tmp_db

    # --8<-- [start:add_data_pydantic_model]
    from lancedb.pydantic import LanceModel, Vector

    # Define a Pydantic model
    class Content(LanceModel):
        movie_id: int
        vector: Vector(128)
        genres: str
        title: str
        imdb_id: int

        @property
        def imdb_url(self) -> str:
            return f"https://www.imdb.com/title/tt{self.imdb_id}"

    # Create table with Pydantic model schema
    table_name = "pydantic_example"
    table = db.create_table(table_name, schema=Content, mode="overwrite")
    # --8<-- [end:add_data_pydantic_model]
    assert table.count_rows() == 0


def test_add_data_nested_model(tmp_db):
    db = tmp_db

    # --8<-- [start:add_data_nested_model]
    from lancedb.pydantic import LanceModel, Vector
    from pydantic import BaseModel

    class Document(BaseModel):
        content: str
        source: str

    class NestedSchema(LanceModel):
        id: str
        vector: Vector(128)
        document: Document

    # Create table with nested schema
    table_name = "nested_model_example"
    table = db.create_table(table_name, schema=NestedSchema, mode="overwrite")
    # --8<-- [end:add_data_nested_model]
    assert table.count_rows() == 0


def test_batch_data_insertion(tmp_db):
    db = tmp_db

    # --8<-- [start:batch_data_insertion]
    import pyarrow as pa

    def make_batches():
        for i in range(5):  # Create 5 batches
            yield pa.RecordBatch.from_arrays(
                [
                    pa.array([[3.1, 4.1], [5.9, 26.5]], pa.list_(pa.float32(), 2)),
                    pa.array([f"item{i * 2 + 1}", f"item{i * 2 + 2}"]),
                    pa.array([float((i * 2 + 1) * 10), float((i * 2 + 2) * 10)]),
                ],
                ["vector", "item", "price"],
            )

    schema = pa.schema(
        [
            pa.field("vector", pa.list_(pa.float32(), 2)),
            pa.field("item", pa.utf8()),
            pa.field("price", pa.float32()),
        ]
    )
    # Create table with batches
    table_name = "batch_ingestion_example"
    table = db.create_table(table_name, make_batches(), schema=schema, mode="overwrite")
    # --8<-- [end:batch_data_insertion]
    assert table.count_rows() == 10


def _create_users_example_table(db, table_name="users_example"):
    return db.create_table(
        table_name,
        data=pa.table(
            {
                "id": [1, 2, 3],
                "name": ["Alice", "Bob", "Charlie"],
                "login_count": [10, 20, 5],
            }
        ),
        mode="overwrite",
    )


def test_update_example_table_setup(tmp_db):
    db = tmp_db

    # --8<-- [start:update_example_table_setup]
    import pyarrow as pa

    table = db.create_table(
        "users_example",
        data=pa.table(
            {
                "id": [1, 2],
                "name": ["Alice", "Bob"],
                "login_count": [10, 20],
            }
        ),
        mode="overwrite",
    )
    # --8<-- [end:update_example_table_setup]
    assert table.count_rows() == 2


def test_update_operation(tmp_db):
    db = tmp_db

    # --8<-- [start:update_operation]
    import pyarrow as pa

    table = db.create_table(
        "users_example",
        data=pa.table(
            {
                "id": [1, 2],
                "name": ["Alice", "Bob"],
                "login_count": [10, 20],
            }
        ),
        mode="overwrite",
    )
    table.update(where="id = 2", values={"name": "Bobby"})
    # --8<-- [end:update_operation]
    rows = table.to_arrow().sort_by("id").to_pylist()
    assert rows == [
        {"id": 1, "name": "Alice", "login_count": 10},
        {"id": 2, "name": "Bobby", "login_count": 20},
    ]


def test_update_using_sql(tmp_db):
    db = tmp_db

    # --8<-- [start:update_using_sql]
    import pyarrow as pa

    table = db.create_table(
        "users_example",
        data=pa.table(
            {
                "id": [1, 2],
                "name": ["Alice", "Bob"],
                "login_count": [10, 20],
            }
        ),
        mode="overwrite",
    )
    table.update(where="id = 2", values_sql={"login_count": "login_count + 1"})
    # --8<-- [end:update_using_sql]
    rows = table.to_arrow().sort_by("id").to_pylist()
    assert rows == [
        {"id": 1, "name": "Alice", "login_count": 10},
        {"id": 2, "name": "Bob", "login_count": 21},
    ]


def test_merge_matched_update_only(tmp_db):
    db = tmp_db

    # --8<-- [start:merge_matched_update_only]
    import pyarrow as pa

    table = db.create_table(
        "users_example",
        data=pa.table(
            {
                "id": [1, 2],
                "name": ["Alice", "Bob"],
                "login_count": [10, 20],
            }
        ),
        mode="overwrite",
    )

    incoming_users = pa.table(
        {
            "id": [2, 3],
            "name": ["Bobby", "Charlie"],
            "login_count": [21, 5],
        }
    )

    (table.merge_insert("id").when_matched_update_all().execute(incoming_users))
    # --8<-- [end:merge_matched_update_only]
    rows = table.to_arrow().sort_by("id").to_pylist()
    assert rows == [
        {"id": 1, "name": "Alice", "login_count": 10},
        {"id": 2, "name": "Bobby", "login_count": 21},
    ]


def test_insert_if_not_exists(tmp_db):
    db = tmp_db

    # --8<-- [start:insert_if_not_exists]
    import pyarrow as pa

    table = db.create_table(
        "users_example",
        data=pa.table(
            {
                "id": [1, 2],
                "name": ["Alice", "Bob"],
                "login_count": [10, 20],
            }
        ),
        mode="overwrite",
    )

    incoming_users = pa.table(
        {
            "id": [2, 3],
            "name": ["Bobby", "Charlie"],
            "login_count": [21, 5],
        }
    )

    (table.merge_insert("id").when_not_matched_insert_all().execute(incoming_users))
    # --8<-- [end:insert_if_not_exists]
    rows = table.to_arrow().sort_by("id").to_pylist()
    assert rows == [
        {"id": 1, "name": "Alice", "login_count": 10},
        {"id": 2, "name": "Bob", "login_count": 20},
        {"id": 3, "name": "Charlie", "login_count": 5},
    ]


def test_merge_update_insert(tmp_db):
    db = tmp_db

    # --8<-- [start:merge_update_insert]
    import pyarrow as pa

    table = db.create_table(
        "users_example",
        data=pa.table(
            {
                "id": [1, 2],
                "name": ["Alice", "Bob"],
                "login_count": [10, 20],
            }
        ),
        mode="overwrite",
    )

    incoming_users = pa.table(
        {
            "id": [2, 3],
            "name": ["Bobby", "Charlie"],
            "login_count": [21, 5],
        }
    )

    (
        table.merge_insert("id")
        .when_matched_update_all()
        .when_not_matched_insert_all()
        .execute(incoming_users)
    )
    # --8<-- [end:merge_update_insert]
    rows = table.to_arrow().sort_by("id").to_pylist()
    assert rows == [
        {"id": 1, "name": "Alice", "login_count": 10},
        {"id": 2, "name": "Bobby", "login_count": 21},
        {"id": 3, "name": "Charlie", "login_count": 5},
    ]


def test_merge_delete_missing_by_source(tmp_db):
    db = tmp_db

    # --8<-- [start:merge_delete_missing_by_source]
    import pyarrow as pa

    table = db.create_table(
        "users_example",
        data=pa.table(
            {
                "id": [1, 2, 3],
                "name": ["Alice", "Bob", "Charlie"],
                "login_count": [10, 20, 5],
            }
        ),
        mode="overwrite",
    )

    incoming_users = pa.table(
        {
            "id": [2, 3],
            "name": ["Bobby", "Charlie"],
            "login_count": [21, 5],
        }
    )

    (
        table.merge_insert("id")
        .when_matched_update_all()
        .when_not_matched_insert_all()
        .when_not_matched_by_source_delete()
        .execute(incoming_users)
    )
    # --8<-- [end:merge_delete_missing_by_source]
    rows = table.to_arrow().sort_by("id").to_pylist()
    assert rows == [
        {"id": 2, "name": "Bobby", "login_count": 21},
        {"id": 3, "name": "Charlie", "login_count": 5},
    ]


def test_merge_partial_columns(tmp_db):
    db = tmp_db

    # --8<-- [start:merge_partial_columns]
    import pyarrow as pa

    table = db.create_table(
        "users_example",
        data=pa.table(
            {
                "id": [1, 2],
                "name": ["Alice", "Bob"],
                "login_count": [10, 20],
            }
        ),
        mode="overwrite",
    )

    incoming_users = pa.table(
        {
            "id": [2, 3],
            "name": ["Bobby", "Charlie"],
        }
    )

    (
        table.merge_insert("id")
        .when_matched_update_all()
        .when_not_matched_insert_all()
        .execute(incoming_users)
    )
    # --8<-- [end:merge_partial_columns]
    rows = table.to_arrow().sort_by("id").to_pylist()
    assert rows == [
        {"id": 1, "name": "Alice", "login_count": 10},
        {"id": 2, "name": "Bobby", "login_count": 20},
        {"id": 3, "name": "Charlie", "login_count": None},
    ]


def test_delete_operation(tmp_db):
    db = tmp_db
    table = _create_users_example_table(db)

    # --8<-- [start:delete_operation]
    # delete data
    predicate = "id = 3"
    table.delete(predicate)
    # --8<-- [end:delete_operation]
    assert table.count_rows() == 2


def test_update_optimize_cleanup_snippet(tmp_db):
    table = _create_users_example_table(tmp_db, table_name="users_cleanup_example")

    # --8<-- [start:update_optimize_cleanup]
    from datetime import timedelta

    table.optimize(cleanup_older_than=timedelta(days=1))
    # --8<-- [end:update_optimize_cleanup]


# ============================================================================
# Schema Evolution Examples
# ============================================================================


def _setup_schema_add_table(tmp_db, data=None):
    # --8<-- [start:schema_add_setup]
    table_name = "schema_evolution_add_example"
    if data is None:
        data = [
            {
                "id": 1,
                "name": "Laptop",
                "price": 1200.00,
                "vector": np.random.random(128).tolist(),
            },
            {
                "id": 2,
                "name": "Smartphone",
                "price": 800.00,
                "vector": np.random.random(128).tolist(),
            },
            {
                "id": 3,
                "name": "Headphones",
                "price": 150.00,
                "vector": np.random.random(128).tolist(),
            },
        ]
    table = tmp_db.create_table(table_name, data, mode="overwrite")
    # --8<-- [end:schema_add_setup]
    return table


def _setup_schema_alter_table(tmp_db, data=None):
    # --8<-- [start:schema_alter_setup]
    table_name = "schema_evolution_alter_example"
    if data is None:
        data = [
            {
                "id": 1,
                "name": "Laptop",
                "price": 1200,
                "discount_price": 1080.0,
                "vector": np.random.random(128).tolist(),
            },
            {
                "id": 2,
                "name": "Smartphone",
                "price": 800,
                "discount_price": 720.0,
                "vector": np.random.random(128).tolist(),
            },
        ]
    schema = pa.schema(
        {
            "id": pa.int64(),
            "name": pa.string(),
            "price": pa.int32(),
            "discount_price": pa.float64(),
            "vector": pa.list_(pa.float32(), 128),
        }
    )
    table = tmp_db.create_table(table_name, data, schema=schema, mode="overwrite")
    # --8<-- [end:schema_alter_setup]
    return table


def _setup_schema_drop_table(tmp_db, data=None):
    # --8<-- [start:schema_drop_setup]
    if data is None:
        data = [
            {
                "id": 1,
                "name": "Laptop",
                "price": 1200.00,
                "temp_col1": "X",
                "temp_col2": 100,
                "vector": np.random.random(128).tolist(),
            },
            {
                "id": 2,
                "name": "Smartphone",
                "price": 800.00,
                "temp_col1": "Y",
                "temp_col2": 200,
                "vector": np.random.random(128).tolist(),
            },
            {
                "id": 3,
                "name": "Headphones",
                "price": 150.00,
                "temp_col1": "Z",
                "temp_col2": 300,
                "vector": np.random.random(128).tolist(),
            },
        ]
    table = tmp_db.create_table("schema_evolution_drop_example", data, mode="overwrite")
    # --8<-- [end:schema_drop_setup]
    return table


def test_add_columns_calculated(tmp_db):
    table = _setup_schema_add_table(tmp_db)

    # --8<-- [start:add_columns_calculated]
    # Add a discounted price column (10% discount)
    table.add_columns({"discounted_price": "cast((price * 0.9) as float)"})
    # --8<-- [end:add_columns_calculated]
    assert "discounted_price" in table.schema.names


def test_add_columns_default_values(tmp_db):
    table = _setup_schema_add_table(tmp_db)

    # --8<-- [start:add_columns_default_values]
    # Add a stock status column with default value
    table.add_columns({"in_stock": "cast(true as boolean)"})
    # --8<-- [end:add_columns_default_values]
    assert "in_stock" in table.schema.names


def test_add_columns_nullable(tmp_db):
    table = _setup_schema_add_table(
        tmp_db,
        data=[
            {
                "id": 1,
                "name": "Laptop",
                "price": 1200.00,
                "vector": np.random.random(128).tolist(),
            }
        ],
    )

    # --8<-- [start:add_columns_nullable]
    # Add a nullable timestamp column
    table.add_columns({"last_ordered": "cast(NULL as timestamp)"})
    # --8<-- [end:add_columns_nullable]
    assert "last_ordered" in table.schema.names


def test_add_feature_columns_sql(tmp_db):
    table = _setup_schema_add_table(tmp_db)

    # --8<-- [start:add_feature_columns_sql]
    table.add_columns(
        {
            "price_per_id": "cast(price / id as float)",
            "price_log": "ln(price)",
            "price_score": "cast(price / (price + 100.0) as float)",
        }
    )
    # --8<-- [end:add_feature_columns_sql]
    assert {"price_per_id", "price_log", "price_score"}.issubset(table.schema.names)


def test_alter_columns_rename(tmp_db):
    table = _setup_schema_alter_table(tmp_db)

    # --8<-- [start:alter_columns_rename]
    # Rename discount_price to sale_price
    table.alter_columns({"path": "discount_price", "rename": "sale_price"})
    # --8<-- [end:alter_columns_rename]
    assert "sale_price" in table.schema.names
    assert "discount_price" not in table.schema.names


def test_alter_columns_data_type(tmp_db):
    table = _setup_schema_alter_table(
        tmp_db,
        data=[
            {
                "id": 1,
                "name": "Laptop",
                "price": 1200,
                "discount_price": 1080.0,
                "vector": np.random.random(128).tolist(),
            }
        ],
    )

    # --8<-- [start:alter_columns_data_type]
    # Change price from int32 to int64 for larger numbers
    table.alter_columns({"path": "price", "data_type": pa.int64()})
    # --8<-- [end:alter_columns_data_type]
    assert table.schema.field("price").type == pa.int64()


def test_alter_columns_nullable(tmp_db):
    table = _setup_schema_alter_table(
        tmp_db,
        data=[
            {
                "id": 1,
                "name": "Laptop",
                "price": 1200,
                "discount_price": 1080.0,
                "vector": np.random.random(128).tolist(),
            }
        ],
    )

    # --8<-- [start:alter_columns_nullable]
    # Make the name column nullable
    table.alter_columns({"path": "name", "nullable": True})
    # --8<-- [end:alter_columns_nullable]
    assert table.schema.field("name").nullable is True


def test_alter_columns_multiple(tmp_db):
    table = _setup_schema_alter_table(
        tmp_db,
        data=[
            {
                "id": 1,
                "name": "Laptop",
                "price": 1200,
                "discount_price": 1080.0,
                "vector": np.random.random(128).tolist(),
            }
        ],
    )
    table.alter_columns({"path": "discount_price", "rename": "sale_price"})

    # --8<-- [start:alter_columns_multiple]
    # Rename, change type, and make nullable in one operation
    table.alter_columns(
        {
            "path": "sale_price",
            "rename": "final_price",
            "data_type": pa.float64(),
            "nullable": True,
        }
    )
    # --8<-- [end:alter_columns_multiple]
    assert "final_price" in table.schema.names
    assert table.schema.field("final_price").nullable is True


def test_alter_columns_with_expression(tmp_db):
    # --8<-- [start:alter_columns_with_expression]
    # For custom transforms, create a new column from a SQL expression.
    expression_table = tmp_db.create_table(
        "schema_evolution_expression_example",
        [{"id": 1, "price_text": "$100"}],
        mode="overwrite",
    )

    expression_table.add_columns(
        {"price_numeric": "cast(replace(price_text, '$', '') as int)"}
    )
    expression_table.drop_columns(["price_text"])
    expression_table.alter_columns({"path": "price_numeric", "rename": "price"})
    # --8<-- [end:alter_columns_with_expression]
    assert "price" in expression_table.schema.names


def test_drop_columns_single(tmp_db):
    table = _setup_schema_drop_table(tmp_db)

    # --8<-- [start:drop_columns_single]
    # Remove the first temporary column
    table.drop_columns(["temp_col1"])
    # --8<-- [end:drop_columns_single]
    assert "temp_col1" not in table.schema.names


def test_drop_columns_multiple(tmp_db):
    table = _setup_schema_drop_table(
        tmp_db,
        data=[
            {
                "id": 1,
                "name": "Laptop",
                "price": 1200.00,
                "temp_col1": "X",
                "temp_col2": 100,
                "vector": np.random.random(128).tolist(),
            },
        ],
    )

    # --8<-- [start:drop_columns_multiple]
    # Remove the second temporary column
    table.drop_columns(["temp_col2"])
    # --8<-- [end:drop_columns_multiple]
    assert "temp_col2" not in table.schema.names


def test_alter_vector_column(tmp_db):
    # --8<-- [start:alter_vector_column]
    vector_dim = 768  # Your embedding dimension
    table_name = "vector_alter_example"
    db = tmp_db
    data = [
        {
            "id": 1,
            "embedding": np.random.random(vector_dim).tolist(),
        },
    ]
    table = db.create_table(table_name, data, mode="overwrite")

    table.alter_columns(
        dict(path="embedding", data_type=pa.list_(pa.float32(), vector_dim))
    )
    # --8<-- [end:alter_vector_column]


def test_schema_field_metadata(tmp_db):
    table = tmp_db.create_table(
        "schema_field_metadata_example",
        pa.table({"id": [0, 1], "category": ["a", "b"]}),
        mode="overwrite",
    )

    # --8<-- [start:schema_field_metadata_merge]
    # Set two metadata keys on the `category` field.
    res = table.update_field_metadata(
        {"path": "category", "metadata": {"unit": "label", "pii": "false"}}
    )
    print(res.version)

    # Merge: add a new key, delete one with None, keep the rest.
    table.update_field_metadata(
        {"path": "category", "metadata": {"source": "import", "pii": None}}
    )

    # Arrow stores field metadata as bytes.
    assert table.schema.field("category").metadata == {
        b"unit": b"label",
        b"source": b"import",
    }
    # --8<-- [end:schema_field_metadata_merge]

    # --8<-- [start:schema_field_metadata_replace]
    table.update_field_metadata(
        {
            "path": "category",
            "metadata": {"owner": "search-team"},
            "replace": True,
        }
    )
    # --8<-- [end:schema_field_metadata_replace]

    assert table.schema.field("category").metadata == {b"owner": b"search-team"}


# ============================================================================
# Versioning Examples
# ============================================================================


def _setup_versioning_table(tmp_db, data=None, table_name="quotes_versioning_example"):
    import pyarrow as pa

    if data is None:
        data = [
            {"id": 1, "author": "Richard", "quote": "Wubba Lubba Dub Dub!"},
            {"id": 2, "author": "Morty", "quote": "Rick, what's going on?"},
            {
                "id": 3,
                "author": "Richard",
                "quote": "I turned myself into a pickle, Morty!",
            },
        ]
    schema = pa.schema(
        [
            pa.field("id", pa.int64()),
            pa.field("author", pa.string()),
            pa.field("quote", pa.string()),
        ]
    )
    return tmp_db.create_table(table_name, data, schema=schema, mode="overwrite")


def test_versioning_basic_setup(tmp_db):
    # --8<-- [start:versioning_basic_setup]
    import pyarrow as pa

    db = tmp_db

    table_name = "quotes_versioning_example"
    data = [
        {"id": 1, "author": "Richard", "quote": "Wubba Lubba Dub Dub!"},
        {"id": 2, "author": "Morty", "quote": "Rick, what's going on?"},
        {
            "id": 3,
            "author": "Richard",
            "quote": "I turned myself into a pickle, Morty!",
        },
    ]

    # Define schema
    schema = pa.schema(
        [
            pa.field("id", pa.int64()),
            pa.field("author", pa.string()),
            pa.field("quote", pa.string()),
        ]
    )

    table = db.create_table(table_name, data, schema=schema, mode="overwrite")
    # --8<-- [end:versioning_basic_setup]
    assert table.count_rows() == 3


def test_versioning_check_initial_version(tmp_db):
    table = _setup_versioning_table(tmp_db)

    # --8<-- [start:versioning_check_initial_version]
    versions = table.list_versions()
    current_version = table.version
    print(f"Number of versions after creation: {len(versions)}")
    print(f"Current version: {current_version}")
    # --8<-- [end:versioning_check_initial_version]
    assert len(versions) == 1
    assert current_version == versions[-1]["version"]


def test_versioning_flow(tmp_db):
    table = _setup_versioning_table(tmp_db)

    # --8<-- [start:versioning_update_data]
    table.update(where="author='Richard'", values={"author": "Richard Daniel Sanchez"})
    rows_after_update = table.count_rows("author = 'Richard Daniel Sanchez'")
    print(f"Rows updated to Richard Daniel Sanchez: {rows_after_update}")
    # --8<-- [end:versioning_update_data]
    assert rows_after_update == 2

    # --8<-- [start:versioning_add_data]
    more_data = [
        {
            "id": 4,
            "author": "Richard Daniel Sanchez",
            "quote": "That's the way the news goes!",
        },
        {"id": 5, "author": "Morty", "quote": "Aww geez, Rick!"},
    ]
    table.add(more_data)
    # --8<-- [end:versioning_add_data]
    assert table.count_rows() == 5

    # --8<-- [start:versioning_check_versions_after_mod]
    versions = table.list_versions()
    version_count_after_mod = len(versions)
    version_after_mod = table.version
    print(f"Number of versions after modifications: {version_count_after_mod}")
    print(f"Current version: {version_after_mod}")
    # --8<-- [end:versioning_check_versions_after_mod]
    assert version_count_after_mod >= 2
    assert version_after_mod == versions[-1]["version"]

    # --8<-- [start:versioning_list_all_versions]
    versions = table.list_versions()
    for v in versions:
        print(f"Version {v['version']}, created at {v['timestamp']}")
    # --8<-- [end:versioning_list_all_versions]
    assert len(versions) >= 1

    # --8<-- [start:versioning_rollback]
    table.restore(version_after_mod)
    versions = table.list_versions()
    version_count_after_rollback = len(versions)
    print(f"Total number of versions after rollback: {version_count_after_rollback}")
    # --8<-- [end:versioning_rollback]
    assert version_count_after_rollback == version_count_after_mod + 1
    assert table.version == versions[-1]["version"]
    assert table.count_rows() == 5

    # --8<-- [start:versioning_checkout_latest]
    table.checkout_latest()
    # --8<-- [end:versioning_checkout_latest]
    assert table.version == table.list_versions()[-1]["version"]

    # --8<-- [start:versioning_delete_data]
    table.delete("author = 'Morty'")
    rows_after_deletion = table.count_rows()
    print(f"Number of rows after deletion: {rows_after_deletion}")
    # --8<-- [end:versioning_delete_data]
    assert rows_after_deletion == 3


def test_versioning_tags(tmp_db):
    import pyarrow as pa

    schema = pa.schema(
        [
            pa.field("id", pa.int64()),
            pa.field("author", pa.string()),
            pa.field("quote", pa.string()),
        ]
    )
    table = tmp_db.create_table(
        "quotes_tags_example",
        [{"id": 1, "author": "Richard", "quote": "Wubba Lubba Dub Dub!"}],
        schema=schema,
        mode="overwrite",
    )  # v1
    table.add([{"id": 2, "author": "Morty", "quote": "Aww geez, Rick!"}])  # v2
    table.add([{"id": 3, "author": "Summer", "quote": "Whatever, Grandpa"}])  # v3

    # --8<-- [start:versioning_tags]
    # Create a tag pointing at a specific version
    table.tags.create("baseline", 1)
    table.tags.create("with-edits", table.version)

    # List all tags on this table
    print(table.tags.list())

    # Look up the version a tag points at
    print(table.tags.get_version("baseline"))

    # Move an existing tag to a different version
    table.tags.update("baseline", 2)

    # Check out a version by tag name
    table.checkout("baseline")
    print(table.version)

    # Delete a tag (does not delete the underlying version)
    table.tags.delete("with-edits")

    # Return to the latest version
    table.checkout_latest()
    # --8<-- [end:versioning_tags]
    assert table.version == 3
    assert "baseline" in table.tags.list()
    assert "with-edits" not in table.tags.list()


# ============================================================================
# Branches Examples
# ============================================================================


def test_branches(tmp_db):
    import numpy as np
    import pyarrow as pa
    from lancedb.index import FTS, IvfPq

    db = tmp_db
    schema = pa.schema(
        [
            pa.field("id", pa.int64()),
            pa.field("author", pa.string()),
            pa.field("quote", pa.string()),
        ]
    )
    table = db.create_table(
        "quotes_branches_example",
        [
            {"id": 1, "author": "Lancelot", "quote": "My lance never fails."},
            {"id": 2, "author": "Arthur", "quote": "Long live Camelot!"},
            {"id": 3, "author": "Merlin", "quote": "Magic always has a price."},
        ],
        schema=schema,
        mode="overwrite",
    )

    # --8<-- [start:branch_create]
    # Fork an isolated, writable branch from main's latest version.
    # `create` returns a table handle scoped to the new branch.
    branch = table.branches.create("exp")
    # --8<-- [end:branch_create]

    # --8<-- [start:branch_write]
    # Writes land on the branch handle only; main is left untouched.
    branch.add([{"id": 4, "author": "Lancelot", "quote": "For the realm!"}])
    print(branch.count_rows())  # 4 rows on the branch
    print(table.count_rows())  # 3 rows; main is unaffected

    # List every branch, each mapped to its metadata (including its fork point).
    print(table.branches.list())
    # --8<-- [end:branch_write]

    # --8<-- [start:branch_reopen]
    # Reopen an existing branch by name from the table handle...
    checked_out = table.branches.checkout("exp")
    # ...or open it directly from the database connection.
    branch_handle = db.open_table("quotes_branches_example", branch="exp")
    print(checked_out.count_rows(), branch_handle.count_rows())  # both 4
    # --8<-- [end:branch_reopen]

    # --8<-- [start:branch_delete]
    # Delete the branch and its branch-local history. Data on main is safe.
    table.branches.delete("exp")
    # --8<-- [end:branch_delete]

    assert table.count_rows() == 3
    assert "exp" not in table.branches.list()

    # Setup: a branch with row results that we want to apply to main.
    candidate = table.branches.create("candidate")
    candidate.update(where="id = 1", values={"quote": "Revised on the branch"})
    candidate.add(
        [{"id": 4, "author": "Galahad", "quote": "The grail awaits."}]
    )

    # --8<-- [start:branch_upsert_to_main]
    # This is a row-level upsert, not a merge of branch histories.
    # `merge_insert` updates matching rows and inserts new rows using a stable
    # unique key. Filter the branch read if you only want to apply some results.
    rows_to_apply = candidate.to_arrow()
    (
        table.merge_insert("id")
        .when_matched_update_all()  # update rows that already exist on main
        .when_not_matched_insert_all()  # insert rows that are new on the branch
        .execute(rows_to_apply)
    )
    # --8<-- [end:branch_upsert_to_main]

    main_rows = {
        row["id"]: row["quote"] for row in table.to_arrow().to_pylist()
    }
    assert table.count_rows() == 4
    assert main_rows[1] == "Revised on the branch"
    assert 4 in main_rows
    table.branches.delete("candidate")

    # Setup: a larger table with a vector and a text column to index.
    rng = np.random.default_rng(0)
    products = db.create_table(
        "products_branch_index",
        [
            {"id": i, "vector": rng.random(4).tolist(), "text": f"product number {i}"}
            for i in range(512)
        ],
        mode="overwrite",
    )

    # --8<-- [start:branch_index]
    # Build and validate indexes on a branch before using the configuration on
    # main.
    dev = products.branches.create("index-dev")

    # A vector (ANN) index and a full-text search index, both branch-scoped.
    dev.create_index(
        "vector",
        config=IvfPq(distance_type="cosine", num_partitions=1, num_sub_vectors=2),
    )
    dev.create_index("text", config=FTS())

    # Both indexes live only on the branch; main still has none.
    print([ix.name for ix in dev.list_indices()])  # branch: two indexes
    print([ix.name for ix in products.list_indices()])  # main: [] (untouched)
    # --8<-- [end:branch_index]

    assert len(dev.list_indices()) == 2
    assert len(products.list_indices()) == 0
    products.branches.delete("index-dev")


# ============================================================================
# Consistency Examples
# ============================================================================


def test_consistency_strong(tmp_db):
    # --8<-- [start:consistency_strong]
    from datetime import timedelta

    uri = str(tmp_db.uri)
    writer_db = lancedb.connect(uri)
    reader_db = lancedb.connect(uri, read_consistency_interval=timedelta(0))
    writer_table = writer_db.create_table(
        "consistency_strong_table", [{"id": 1}], mode="overwrite"
    )
    reader_table = reader_db.open_table("consistency_strong_table")
    writer_table.add([{"id": 2}])
    rows_after_write = reader_table.count_rows()
    print(f"Rows visible with strong consistency: {rows_after_write}")
    # --8<-- [end:consistency_strong]
    assert rows_after_write == 2


def test_consistency_eventual(tmp_db):
    # --8<-- [start:consistency_eventual]
    from datetime import timedelta

    uri = str(tmp_db.uri)
    writer_db = lancedb.connect(uri)
    reader_db = lancedb.connect(uri, read_consistency_interval=timedelta(seconds=3600))
    writer_table = writer_db.create_table(
        "consistency_eventual_table", [{"id": 1}], mode="overwrite"
    )
    reader_table = reader_db.open_table("consistency_eventual_table")
    writer_table.add([{"id": 2}])
    rows_after_write = reader_table.count_rows()
    print(f"Rows visible before eventual refresh interval: {rows_after_write}")
    # --8<-- [end:consistency_eventual]
    assert rows_after_write == 1


def test_consistency_checkout_latest(tmp_db):
    # --8<-- [start:consistency_checkout_latest]
    uri = str(tmp_db.uri)
    writer_db = lancedb.connect(uri)
    reader_db = lancedb.connect(uri)
    writer_table = writer_db.create_table(
        "consistency_checkout_latest_table", [{"id": 1}], mode="overwrite"
    )
    reader_table = reader_db.open_table("consistency_checkout_latest_table")

    writer_table.add([{"id": 2}])
    rows_before_refresh = reader_table.count_rows()
    print(f"Rows before checkout_latest: {rows_before_refresh}")

    reader_table.checkout_latest()
    rows_after_refresh = reader_table.count_rows()
    print(f"Rows after checkout_latest: {rows_after_refresh}")
    # --8<-- [end:consistency_checkout_latest]
    assert rows_before_refresh == 1
    assert rows_after_refresh == 2
