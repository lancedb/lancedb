# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors


def test_create_table_from_dicts():
    # --8<-- [start:create_table_from_dicts]
    from tempfile import TemporaryDirectory

    import lancedb

    with TemporaryDirectory(prefix="lancedb-ingestion-") as uri:
        db = lancedb.connect(uri)
        data = [
            {"vector": [1.1, 1.2], "lat": 45.5, "long": -122.7},
            {"vector": [0.2, 1.8], "lat": 40.1, "long": -74.1},
        ]
        db.create_table("test_table", data, mode="overwrite")
        tbl = db["test_table"]
        tbl.head()

        print("Rows:", tbl.count_rows())
        # --8<-- [end:create_table_from_dicts]
        assert tbl.count_rows() == 2


def test_create_table_conflict_handling():
    # --8<-- [start:create_table_conflict_handling]
    from tempfile import TemporaryDirectory

    import lancedb

    with TemporaryDirectory(prefix="lancedb-ingestion-") as uri:
        db = lancedb.connect(uri)
        data = [{"vector": [1.1, 1.2], "lat": 45.5, "long": -122.7}]
        db.create_table("conflict_table", data)
        # Idempotent open: reuse the existing table if it exists.
        # The provided data is ignored; the schema is validated against the
        # existing table and a mismatch raises an error.
        tbl = db.create_table("conflict_table", data, exist_ok=True)

        # Overwrite: replace the contents with the provided data.
        tbl = db.create_table("conflict_table", data, mode="overwrite")
        print("Rows:", tbl.count_rows())
    # --8<-- [end:create_table_conflict_handling]


def test_create_table_custom_schema():
    # --8<-- [start:create_table_custom_schema]
    from tempfile import TemporaryDirectory

    import lancedb

    with TemporaryDirectory(prefix="lancedb-ingestion-") as uri:
        db = lancedb.connect(uri)
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
        tbl = db.create_table(
            "my_table_custom_schema", data, schema=custom_schema, mode="overwrite"
        )

        print("Rows:", tbl.count_rows())
        # --8<-- [end:create_table_custom_schema]
        assert tbl.count_rows() == 2


def test_create_table_from_pandas():
    # --8<-- [start:create_table_from_pandas]
    from tempfile import TemporaryDirectory

    import lancedb

    with TemporaryDirectory(prefix="lancedb-ingestion-") as uri:
        db = lancedb.connect(uri)
        import pandas as pd

        data = pd.DataFrame(
            {
                "vector": [[1.1, 1.2, 1.3, 1.4], [0.2, 1.8, 0.4, 3.6]],
                "lat": [45.5, 40.1],
                "long": [-122.7, -74.1],
            }
        )
        db.create_table("my_table_pandas", data, mode="overwrite")
        db["my_table_pandas"].head()
    # --8<-- [end:create_table_from_pandas]


def test_create_table_from_polars():
    # --8<-- [start:create_table_from_polars]
    from tempfile import TemporaryDirectory

    import lancedb

    with TemporaryDirectory(prefix="lancedb-ingestion-") as uri:
        db = lancedb.connect(uri)
        import polars as pl

        data = pl.DataFrame(
            {
                "vector": [[3.1, 4.1], [5.9, 26.5]],
                "item": ["foo", "bar"],
                "price": [10.0, 20.0],
            }
        )
        tbl = db.create_table("my_table_pl", data, mode="overwrite")

        print("Rows:", tbl.count_rows())
        # --8<-- [end:create_table_from_polars]
        assert tbl.count_rows() == 2


def test_create_table_from_arrow():
    # --8<-- [start:create_table_from_arrow]
    from tempfile import TemporaryDirectory

    import lancedb

    with TemporaryDirectory(prefix="lancedb-ingestion-") as uri:
        db = lancedb.connect(uri)
        import numpy as np
        import pyarrow as pa

        dim = 16
        total = 2
        schema = pa.schema(
            [
                pa.field("vector", pa.list_(pa.float16(), dim)),
                pa.field("text", pa.string()),
            ]
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
        tbl = db.create_table("f16_tbl", data, schema=schema, mode="overwrite")

        print("Rows:", tbl.count_rows())
        # --8<-- [end:create_table_from_arrow]
        assert tbl.count_rows() == 2


def test_create_table_from_pydantic():
    # --8<-- [start:create_table_from_pydantic]
    from tempfile import TemporaryDirectory

    import lancedb

    with TemporaryDirectory(prefix="lancedb-ingestion-") as uri:
        db = lancedb.connect(uri)
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

        tbl = db.create_table("movielens_small", schema=Content, mode="overwrite")

        print("Rows:", tbl.count_rows())
        # --8<-- [end:create_table_from_pydantic]
        assert tbl.count_rows() == 0


def test_create_table_nested_schema():
    # --8<-- [start:create_table_nested_schema]
    from tempfile import TemporaryDirectory

    import lancedb

    with TemporaryDirectory(prefix="lancedb-ingestion-") as uri:
        db = lancedb.connect(uri)
        from lancedb.pydantic import LanceModel, Vector
        from pydantic import BaseModel

        class Document(BaseModel):
            content: str
            source: str

        class NestedSchema(LanceModel):
            id: str
            vector: Vector(1536)
            document: Document

        tbl = db.create_table("nested_table", schema=NestedSchema, mode="overwrite")

        print("Rows:", tbl.count_rows())
        # --8<-- [end:create_table_nested_schema]
        assert tbl.count_rows() == 0


def test_create_empty_table():
    # --8<-- [start:create_empty_table]
    from tempfile import TemporaryDirectory

    import lancedb

    with TemporaryDirectory(prefix="lancedb-ingestion-") as uri:
        db = lancedb.connect(uri)
        import pyarrow as pa

        schema = pa.schema(
            [
                pa.field("vector", pa.list_(pa.float32(), 2)),
                pa.field("item", pa.string()),
                pa.field("price", pa.float32()),
            ]
        )
        tbl = db.create_table("test_empty_table", schema=schema, mode="overwrite")

        print("Rows:", tbl.count_rows())
        # --8<-- [end:create_empty_table]
        assert tbl.count_rows() == 0


def test_create_empty_table_pydantic():
    # --8<-- [start:create_empty_table_pydantic]
    from tempfile import TemporaryDirectory

    import lancedb

    with TemporaryDirectory(prefix="lancedb-ingestion-") as uri:
        db = lancedb.connect(uri)
        from lancedb.pydantic import LanceModel, Vector

        class Item(LanceModel):
            vector: Vector(2)
            item: str
            price: float

        tbl = db.create_table(
            "test_empty_table_new", schema=Item.to_arrow_schema(), mode="overwrite"
        )

        print("Rows:", tbl.count_rows())
        # --8<-- [end:create_empty_table_pydantic]
        assert tbl.count_rows() == 0


def test_open_existing_table():
    # --8<-- [start:open_existing_table]
    from tempfile import TemporaryDirectory

    import lancedb

    with TemporaryDirectory(prefix="lancedb-ingestion-") as uri:
        db = lancedb.connect(uri)
        # Create a table first
        data = [{"vector": [1.1, 1.2], "lat": 45.5, "long": -122.7}]
        db.create_table("test_table", data, mode="overwrite")

        # List table names
        print(db.list_tables().tables)

        # Open existing table
        tbl = db.open_table("test_table")

        print("Rows:", tbl.count_rows())
        # --8<-- [end:open_existing_table]
        assert tbl.count_rows() == 1


def test_drop_table():
    # --8<-- [start:drop_table]
    from tempfile import TemporaryDirectory

    import lancedb

    with TemporaryDirectory(prefix="lancedb-ingestion-") as uri:
        db = lancedb.connect(uri)
        # Create a table first
        data = [{"vector": [1.1, 1.2], "lat": 45.5}]
        db.create_table("my_table", data, mode="overwrite")

        # Drop the table
        db.drop_table("my_table")
    # --8<-- [end:drop_table]


def test_add_from_dataset():
    # --8<-- [start:add_from_dataset]
    from tempfile import TemporaryDirectory

    import lancedb

    with TemporaryDirectory(prefix="lancedb-ingestion-") as uri:
        db = lancedb.connect(uri)
        from pathlib import Path

        import pyarrow as pa
        import pyarrow.parquet as pq

        data_path = str(Path(uri) / "input.parquet")
        pq.write_table(pa.table({"id": [1, 2], "name": ["Alice", "Bob"]}), data_path)
        import pyarrow.dataset as ds

        dataset = ds.dataset(data_path, format="parquet")
        table = db.create_table("my_table", schema=dataset.schema, mode="overwrite")
        table.add(dataset)

        print("Rows:", table.count_rows())
        # --8<-- [end:add_from_dataset]
        assert table.count_rows() == 2


def test_create_table_from_iterator():
    # --8<-- [start:create_table_from_iterator]
    from tempfile import TemporaryDirectory

    import lancedb

    with TemporaryDirectory(prefix="lancedb-ingestion-") as uri:
        db = lancedb.connect(uri)
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

        table = db.create_table("batched_table", schema=schema)
        table.add(make_batches())

        print("Rows:", table.count_rows())
        # --8<-- [end:create_table_from_iterator]
        assert table.count_rows() == 10


def test_update_operation():
    # --8<-- [start:update_operation]
    from tempfile import TemporaryDirectory

    import lancedb

    with TemporaryDirectory(prefix="lancedb-ingestion-") as uri:
        db = lancedb.connect(uri)
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

        print("Rows:", table.count_rows())
        # --8<-- [end:update_operation]
        assert table.count_rows() == 2
        assert table.search().where("id = 2").to_list()[0]["name"] == "Bobby"


def test_update_using_sql():
    # --8<-- [start:update_using_sql]
    from tempfile import TemporaryDirectory

    import lancedb

    with TemporaryDirectory(prefix="lancedb-ingestion-") as uri:
        db = lancedb.connect(uri)
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

        print("Rows:", table.count_rows())
        # --8<-- [end:update_using_sql]
        assert table.count_rows() == 2
        assert table.search().where("id = 2").to_list()[0]["login_count"] == 21


def test_merge_matched_update_only():
    # --8<-- [start:merge_matched_update_only]
    from tempfile import TemporaryDirectory

    import lancedb

    with TemporaryDirectory(prefix="lancedb-ingestion-") as uri:
        db = lancedb.connect(uri)
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

        print("Rows:", table.count_rows())
        # --8<-- [end:merge_matched_update_only]
        assert table.count_rows() == 2


def test_insert_if_not_exists():
    # --8<-- [start:insert_if_not_exists]
    from tempfile import TemporaryDirectory

    import lancedb

    with TemporaryDirectory(prefix="lancedb-ingestion-") as uri:
        db = lancedb.connect(uri)
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

        print("Rows:", table.count_rows())
        # --8<-- [end:insert_if_not_exists]
        assert table.count_rows() == 3


def test_merge_update_insert():
    # --8<-- [start:merge_update_insert]
    from tempfile import TemporaryDirectory

    import lancedb

    with TemporaryDirectory(prefix="lancedb-ingestion-") as uri:
        db = lancedb.connect(uri)
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

        print("Rows:", table.count_rows())
        # --8<-- [end:merge_update_insert]
        assert table.count_rows() == 3
        assert table.search().where("id = 2").to_list()[0]["login_count"] == 21


def test_merge_delete_missing_by_source():
    # --8<-- [start:merge_delete_missing_by_source]
    from tempfile import TemporaryDirectory

    import lancedb

    with TemporaryDirectory(prefix="lancedb-ingestion-") as uri:
        db = lancedb.connect(uri)
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

        print("Rows:", table.count_rows())
        # --8<-- [end:merge_delete_missing_by_source]
        assert table.count_rows() == 2
        assert table.count_rows("id = 1") == 0


def test_merge_partial_columns():
    # --8<-- [start:merge_partial_columns]
    from tempfile import TemporaryDirectory

    import lancedb

    with TemporaryDirectory(prefix="lancedb-ingestion-") as uri:
        db = lancedb.connect(uri)
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

        print("Rows:", table.count_rows())
        # --8<-- [end:merge_partial_columns]
        assert table.count_rows() == 3
        assert table.search().where("id = 3").to_list()[0]["login_count"] is None


def test_write_workflow():
    # --8<-- [start:write_workflow]
    from tempfile import TemporaryDirectory

    import lancedb

    with TemporaryDirectory(prefix="lancedb-ingestion-") as uri:
        db = lancedb.connect(uri)
        table = db.create_table(
            "users",
            [
                {"id": 1, "name": "Alice", "login_count": 10},
                {"id": 2, "name": "Bob", "login_count": 20},
            ],
        )
        table.add([{"id": 3, "name": "Charlie", "login_count": 5}])
        table.update(where="id = 2", values={"name": "Bobby"})
        table.update(where="id = 2", values_sql={"login_count": "login_count + 1"})
        (
            table.merge_insert("id")
            .when_matched_update_all()
            .when_not_matched_insert_all()
            .execute(
                [
                    {"id": 3, "name": "Charlie", "login_count": 6},
                    {"id": 4, "name": "Dana", "login_count": 1},
                ]
            )
        )
        print("Rows selected for deletion:", table.search().where("id = 4").to_list())
        table.delete("id = 4")
        rows = sorted(table.search().to_list(), key=lambda row: row["id"])
        print(rows)
    # --8<-- [end:write_workflow]

    assert rows == [
        {"id": 1, "name": "Alice", "login_count": 10},
        {"id": 2, "name": "Bobby", "login_count": 21},
        {"id": 3, "name": "Charlie", "login_count": 6},
    ]


def test_append_rows():
    # --8<-- [start:append_rows]
    from tempfile import TemporaryDirectory

    import lancedb

    with TemporaryDirectory(prefix="lancedb-ingestion-") as uri:
        db = lancedb.connect(uri)
        table = db.create_table("users", [{"id": 1, "name": "Alice"}])
        table.add([{"id": 2, "name": "Bob"}, {"id": 3, "name": "Charlie"}])
        rows = sorted(table.search().to_list(), key=lambda row: row["id"])
        print(rows)
    # --8<-- [end:append_rows]

    assert [row["id"] for row in rows] == [1, 2, 3]


def test_delete_rows():
    # --8<-- [start:delete_rows]
    from tempfile import TemporaryDirectory

    import lancedb

    with TemporaryDirectory(prefix="lancedb-ingestion-") as uri:
        db = lancedb.connect(uri)
        table = db.create_table(
            "users",
            [
                {"id": 1, "name": "Alice"},
                {"id": 2, "name": "Bob"},
                {"id": 3, "name": "Charlie"},
            ],
        )
        predicate = "id = 3"
        print("Selected:", table.search().where(predicate).to_list())
        table.delete(predicate)
        rows = sorted(table.search().to_list(), key=lambda row: row["id"])
        print("Remaining:", rows)
    # --8<-- [end:delete_rows]

    assert rows == [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]


def test_add_progress():
    # --8<-- [start:add_progress]
    from tempfile import TemporaryDirectory

    import lancedb

    def report_progress(progress):
        print(f"Rows written: {progress['output_rows']}")

    with TemporaryDirectory(prefix="lancedb-ingestion-") as uri:
        db = lancedb.connect(uri)
        table = db.create_table("users", [{"id": 1, "name": "Alice"}])
        table.add(
            [{"id": 2, "name": "Bob"}, {"id": 3, "name": "Charlie"}],
            progress=report_progress,
        )
        rows = sorted(table.search().to_list(), key=lambda row: row["id"])
        print("After insertion:", rows)
    # --8<-- [end:add_progress]

    assert rows == [
        {"id": 1, "name": "Alice"},
        {"id": 2, "name": "Bob"},
        {"id": 3, "name": "Charlie"},
    ]
