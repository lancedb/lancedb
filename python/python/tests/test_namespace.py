# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

"""Tests for LanceDB namespace integration."""

import tempfile
import shutil
import pytest
import pyarrow as pa
import lancedb


class TestNamespaceConnection:
    """Test namespace-based LanceDB connection using DirectoryNamespace."""

    def setup_method(self):
        """Set up test fixtures."""
        self.temp_dir = tempfile.mkdtemp()

    def teardown_method(self):
        """Clean up test fixtures."""
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_connect_namespace_test(self):
        """Test connecting to LanceDB through DirectoryNamespace."""
        # Connect using DirectoryNamespace
        db = lancedb.connect_namespace("dir", {"root": self.temp_dir})

        # Should be a LanceNamespaceDBConnection
        assert isinstance(db, lancedb.LanceNamespaceDBConnection)

        # Initially no tables in root
        assert len(list(db.table_names())) == 0

    def test_create_table_through_namespace(self):
        """Test creating a table through namespace."""
        db = lancedb.connect_namespace("dir", {"root": self.temp_dir})

        # Create a child namespace first
        db.create_namespace(["test_ns"])

        # Define schema for empty table
        schema = pa.schema(
            [
                pa.field("id", pa.int64()),
                pa.field("vector", pa.list_(pa.float32(), 2)),
                pa.field("text", pa.string()),
            ]
        )

        # Create empty table in child namespace
        table = db.create_table("test_table", schema=schema, namespace=["test_ns"])
        assert table is not None
        assert table.name == "test_table"
        assert table.namespace == ["test_ns"]
        assert table.id == "test_ns$test_table"

        # Table should appear in child namespace
        table_names = list(db.table_names(namespace=["test_ns"]))
        assert "test_table" in table_names
        assert len(table_names) == 1

        # Verify empty table
        result = table.to_pandas()
        assert len(result) == 0
        assert list(result.columns) == ["id", "vector", "text"]

    def test_open_table_through_namespace(self):
        """Test opening an existing table through namespace."""
        db = lancedb.connect_namespace("dir", {"root": self.temp_dir})

        # Create a child namespace first
        db.create_namespace(["test_ns"])

        # Create a table with schema in child namespace
        schema = pa.schema(
            [
                pa.field("id", pa.int64()),
                pa.field("vector", pa.list_(pa.float32(), 2)),
            ]
        )
        db.create_table("test_table", schema=schema, namespace=["test_ns"])

        # Open the table
        table = db.open_table("test_table", namespace=["test_ns"])
        assert table is not None
        assert table.name == "test_table"
        assert table.namespace == ["test_ns"]
        assert table.id == "test_ns$test_table"

        # Verify empty table with correct schema
        result = table.to_pandas()
        assert len(result) == 0
        assert list(result.columns) == ["id", "vector"]

    def test_drop_table_through_namespace(self):
        """Test dropping a table through namespace."""
        db = lancedb.connect_namespace("dir", {"root": self.temp_dir})

        # Create a child namespace first
        db.create_namespace(["test_ns"])

        # Create tables in child namespace
        schema = pa.schema(
            [
                pa.field("id", pa.int64()),
                pa.field("vector", pa.list_(pa.float32(), 2)),
            ]
        )
        db.create_table("table1", schema=schema, namespace=["test_ns"])
        db.create_table("table2", schema=schema, namespace=["test_ns"])

        # Verify both tables exist in child namespace
        table_names = list(db.table_names(namespace=["test_ns"]))
        assert "table1" in table_names
        assert "table2" in table_names
        assert len(table_names) == 2

        # Drop one table
        db.drop_table("table1", namespace=["test_ns"])

        # Verify only table2 remains
        table_names = list(db.table_names(namespace=["test_ns"]))
        assert "table1" not in table_names
        assert "table2" in table_names
        assert len(table_names) == 1

        # Drop the second table
        db.drop_table("table2", namespace=["test_ns"])
        assert len(list(db.table_names(namespace=["test_ns"]))) == 0

        # Should not be able to open dropped table
        with pytest.raises(RuntimeError):
            db.open_table("table1", namespace=["test_ns"])

    def test_create_table_with_schema(self):
        """Test creating a table with explicit schema through namespace."""
        db = lancedb.connect_namespace("dir", {"root": self.temp_dir})

        # Create a child namespace first
        db.create_namespace(["test_ns"])

        # Define schema
        schema = pa.schema(
            [
                pa.field("id", pa.int64()),
                pa.field("vector", pa.list_(pa.float32(), 3)),
                pa.field("text", pa.string()),
            ]
        )

        # Create table with schema in child namespace
        table = db.create_table("test_table", schema=schema, namespace=["test_ns"])
        assert table is not None
        assert table.namespace == ["test_ns"]

        # Verify schema
        table_schema = table.schema
        assert len(table_schema) == 3
        assert table_schema.field("id").type == pa.int64()
        assert table_schema.field("text").type == pa.string()

    def test_rename_table_not_supported(self):
        """Test that rename_table raises NotImplementedError."""
        db = lancedb.connect_namespace("dir", {"root": self.temp_dir})

        # Create a child namespace first
        db.create_namespace(["test_ns"])

        # Create a table in child namespace
        schema = pa.schema(
            [
                pa.field("id", pa.int64()),
                pa.field("vector", pa.list_(pa.float32(), 2)),
            ]
        )
        db.create_table("old_name", schema=schema, namespace=["test_ns"])

        # Rename should raise NotImplementedError
        with pytest.raises(NotImplementedError, match="rename_table is not supported"):
            db.rename_table("old_name", "new_name")

    def test_drop_all_tables(self):
        """Test dropping all tables through namespace."""
        db = lancedb.connect_namespace("dir", {"root": self.temp_dir})

        # Create a child namespace first
        db.create_namespace(["test_ns"])

        # Create multiple tables in child namespace
        schema = pa.schema(
            [
                pa.field("id", pa.int64()),
                pa.field("vector", pa.list_(pa.float32(), 2)),
            ]
        )
        for i in range(3):
            db.create_table(f"table{i}", schema=schema, namespace=["test_ns"])

        # Verify tables exist in child namespace
        assert len(list(db.table_names(namespace=["test_ns"]))) == 3

        # Drop all tables in child namespace
        db.drop_all_tables(namespace=["test_ns"])

        # Verify all tables are gone from child namespace
        assert len(list(db.table_names(namespace=["test_ns"]))) == 0

        # Test that table_names works with keyword-only namespace parameter
        db.create_table("test_table", schema=schema, namespace=["test_ns"])
        result = list(db.table_names(namespace=["test_ns"]))
        assert "test_table" in result

    def test_table_operations(self):
        """Test various table operations through namespace."""
        db = lancedb.connect_namespace("dir", {"root": self.temp_dir})

        # Create a child namespace first
        db.create_namespace(["test_ns"])

        # Create a table with schema in child namespace
        schema = pa.schema(
            [
                pa.field("id", pa.int64()),
                pa.field("vector", pa.list_(pa.float32(), 2)),
                pa.field("text", pa.string()),
            ]
        )
        table = db.create_table("test_table", schema=schema, namespace=["test_ns"])

        # Verify empty table was created
        result = table.to_pandas()
        assert len(result) == 0
        assert list(result.columns) == ["id", "vector", "text"]

        # Test add data to the table
        new_data = [
            {"id": 1, "vector": [1.0, 2.0], "text": "item_1"},
            {"id": 2, "vector": [2.0, 3.0], "text": "item_2"},
        ]
        table.add(new_data)
        result = table.to_pandas()
        assert len(result) == 2

        # Test delete
        table.delete("id = 1")
        result = table.to_pandas()
        assert len(result) == 1
        assert result["id"].values[0] == 2

        # Test update
        table.update(where="id = 2", values={"text": "updated"})
        result = table.to_pandas()
        assert result["text"].values[0] == "updated"

    def test_storage_options(self):
        """Test passing storage options through namespace connection."""
        # Connect with storage options
        storage_opts = {"test_option": "test_value"}
        db = lancedb.connect_namespace(
            "dir", {"root": self.temp_dir}, storage_options=storage_opts
        )

        # Storage options should be preserved
        assert db.storage_options == storage_opts

        # Create table with additional storage options
        table_opts = {"table_option": "table_value"}
        schema = pa.schema(
            [
                pa.field("id", pa.int64()),
                pa.field("vector", pa.list_(pa.float32(), 2)),
            ]
        )
        db.create_table("test_table", schema=schema, storage_options=table_opts)

    def test_namespace_operations(self):
        """Test namespace management operations."""
        db = lancedb.connect_namespace("dir", {"root": self.temp_dir})

        # Initially no namespaces
        assert len(list(db.list_namespaces())) == 0

        # Create a namespace
        db.create_namespace(["test_namespace"])

        # Verify namespace exists
        namespaces = list(db.list_namespaces())
        assert "test_namespace" in namespaces
        assert len(namespaces) == 1

        # Create table in namespace
        schema = pa.schema(
            [
                pa.field("id", pa.int64()),
                pa.field("vector", pa.list_(pa.float32(), 2)),
            ]
        )
        table = db.create_table(
            "test_table", schema=schema, namespace=["test_namespace"]
        )
        assert table is not None

        # Verify table exists in namespace
        tables_in_namespace = list(db.table_names(namespace=["test_namespace"]))
        assert "test_table" in tables_in_namespace
        assert len(tables_in_namespace) == 1

        # Open table from namespace
        table = db.open_table("test_table", namespace=["test_namespace"])
        assert table is not None
        assert table.name == "test_table"

        # Drop table from namespace
        db.drop_table("test_table", namespace=["test_namespace"])

        # Verify table no longer exists in namespace
        tables_in_namespace = list(db.table_names(namespace=["test_namespace"]))
        assert len(tables_in_namespace) == 0

        # Drop namespace
        db.drop_namespace(["test_namespace"])

        # Verify namespace no longer exists
        namespaces = list(db.list_namespaces())
        assert len(namespaces) == 0

    def test_namespace_with_tables_cannot_be_dropped(self):
        """Test that namespaces containing tables cannot be dropped."""
        db = lancedb.connect_namespace("dir", {"root": self.temp_dir})

        # Create namespace and table
        db.create_namespace(["test_namespace"])
        schema = pa.schema(
            [
                pa.field("id", pa.int64()),
                pa.field("vector", pa.list_(pa.float32(), 2)),
            ]
        )
        db.create_table("test_table", schema=schema, namespace=["test_namespace"])

        # Try to drop namespace with tables - should fail
        with pytest.raises(RuntimeError, match="is not empty"):
            db.drop_namespace(["test_namespace"])

        # Drop table first
        db.drop_table("test_table", namespace=["test_namespace"])

        # Now dropping namespace should work
        db.drop_namespace(["test_namespace"])

    def test_same_table_name_different_namespaces(self):
        db = lancedb.connect_namespace("dir", {"root": self.temp_dir})

        # Create two namespaces
        db.create_namespace(["namespace_a"])
        db.create_namespace(["namespace_b"])

        # Define schema
        schema = pa.schema(
            [
                pa.field("id", pa.int64()),
                pa.field("vector", pa.list_(pa.float32(), 2)),
                pa.field("text", pa.string()),
            ]
        )

        # Create table with same name in both namespaces
        table_a = db.create_table(
            "same_name_table", schema=schema, namespace=["namespace_a"]
        )
        table_b = db.create_table(
            "same_name_table", schema=schema, namespace=["namespace_b"]
        )

        # Add different data to each table
        data_a = [
            {"id": 1, "vector": [1.0, 2.0], "text": "data_from_namespace_a"},
            {"id": 2, "vector": [3.0, 4.0], "text": "also_from_namespace_a"},
        ]
        table_a.add(data_a)

        data_b = [
            {"id": 10, "vector": [10.0, 20.0], "text": "data_from_namespace_b"},
            {"id": 20, "vector": [30.0, 40.0], "text": "also_from_namespace_b"},
            {"id": 30, "vector": [50.0, 60.0], "text": "more_from_namespace_b"},
        ]
        table_b.add(data_b)

        # Verify data in namespace_a table
        opened_table_a = db.open_table("same_name_table", namespace=["namespace_a"])
        result_a = opened_table_a.to_pandas().sort_values("id").reset_index(drop=True)
        assert len(result_a) == 2
        assert result_a["id"].tolist() == [1, 2]
        assert result_a["text"].tolist() == [
            "data_from_namespace_a",
            "also_from_namespace_a",
        ]
        assert [v.tolist() for v in result_a["vector"]] == [[1.0, 2.0], [3.0, 4.0]]

        # Verify data in namespace_b table
        opened_table_b = db.open_table("same_name_table", namespace=["namespace_b"])
        result_b = opened_table_b.to_pandas().sort_values("id").reset_index(drop=True)
        assert len(result_b) == 3
        assert result_b["id"].tolist() == [10, 20, 30]
        assert result_b["text"].tolist() == [
            "data_from_namespace_b",
            "also_from_namespace_b",
            "more_from_namespace_b",
        ]
        assert [v.tolist() for v in result_b["vector"]] == [
            [10.0, 20.0],
            [30.0, 40.0],
            [50.0, 60.0],
        ]

        # Verify root namespace doesn't have this table
        root_tables = list(db.table_names())
        assert "same_name_table" not in root_tables

        # Clean up
        db.drop_table("same_name_table", namespace=["namespace_a"])
        db.drop_table("same_name_table", namespace=["namespace_b"])
        db.drop_namespace(["namespace_a"])
        db.drop_namespace(["namespace_b"])


@pytest.mark.asyncio
class TestAsyncNamespaceConnection:
    """Test async namespace-based LanceDB connection using DirectoryNamespace."""

    def setup_method(self):
        """Set up test fixtures."""
        self.temp_dir = tempfile.mkdtemp()

    def teardown_method(self):
        """Clean up test fixtures."""
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    async def test_connect_namespace_async(self):
        """Test connecting to LanceDB through DirectoryNamespace asynchronously."""
        # Connect using DirectoryNamespace async (function is sync but returns async connection)
        db = lancedb.connect_namespace_async("dir", {"root": self.temp_dir})

        # Should be an AsyncLanceNamespaceDBConnection
        assert isinstance(db, lancedb.AsyncLanceNamespaceDBConnection)

        # Initially no tables in root
        table_names = await db.table_names()
        assert len(list(table_names)) == 0

    async def test_create_table_async(self):
        """Test creating a table asynchronously through namespace."""
        db = lancedb.connect_namespace_async("dir", {"root": self.temp_dir})

        # Create a child namespace first
        await db.create_namespace(["test_ns"])

        # Define schema for empty table
        schema = pa.schema(
            [
                pa.field("id", pa.int64()),
                pa.field("vector", pa.list_(pa.float32(), 2)),
                pa.field("text", pa.string()),
            ]
        )

        # Create empty table in child namespace
        table = await db.create_table(
            "test_table", schema=schema, namespace=["test_ns"]
        )
        assert table is not None
        assert isinstance(table, lancedb.AsyncTable)

        # Table should appear in child namespace
        table_names = await db.table_names(namespace=["test_ns"])
        assert "test_table" in list(table_names)

    async def test_open_table_async(self):
        """Test opening an existing table asynchronously through namespace."""
        db = lancedb.connect_namespace_async("dir", {"root": self.temp_dir})

        # Create a child namespace first
        await db.create_namespace(["test_ns"])

        # Create a table with schema in child namespace
        schema = pa.schema(
            [
                pa.field("id", pa.int64()),
                pa.field("vector", pa.list_(pa.float32(), 2)),
            ]
        )
        await db.create_table("test_table", schema=schema, namespace=["test_ns"])

        # Open the table
        table = await db.open_table("test_table", namespace=["test_ns"])
        assert table is not None
        assert isinstance(table, lancedb.AsyncTable)

        # Test write operation - add data to the table
        test_data = [
            {"id": 1, "vector": [1.0, 2.0]},
            {"id": 2, "vector": [3.0, 4.0]},
            {"id": 3, "vector": [5.0, 6.0]},
        ]
        await table.add(test_data)

        # Test read operation - query the table
        result = await table.to_arrow()
        assert len(result) == 3
        assert result.schema.field("id").type == pa.int64()
        assert result.schema.field("vector").type == pa.list_(pa.float32(), 2)

        # Verify data content
        result_df = result.to_pandas()
        assert result_df["id"].tolist() == [1, 2, 3]
        assert [v.tolist() for v in result_df["vector"]] == [
            [1.0, 2.0],
            [3.0, 4.0],
            [5.0, 6.0],
        ]

        # Test update operation
        await table.update({"id": 20}, where="id = 2")
        result = await table.to_arrow()
        result_df = result.to_pandas().sort_values("id").reset_index(drop=True)
        assert result_df["id"].tolist() == [1, 3, 20]

        # Test delete operation
        await table.delete("id = 1")
        result = await table.to_arrow()
        assert len(result) == 2
        result_df = result.to_pandas().sort_values("id").reset_index(drop=True)
        assert result_df["id"].tolist() == [3, 20]

    async def test_drop_table_async(self):
        """Test dropping a table asynchronously through namespace."""
        db = lancedb.connect_namespace_async("dir", {"root": self.temp_dir})

        # Create a child namespace first
        await db.create_namespace(["test_ns"])

        # Create tables in child namespace
        schema = pa.schema(
            [
                pa.field("id", pa.int64()),
                pa.field("vector", pa.list_(pa.float32(), 2)),
            ]
        )
        await db.create_table("table1", schema=schema, namespace=["test_ns"])
        await db.create_table("table2", schema=schema, namespace=["test_ns"])

        # Verify both tables exist in child namespace
        table_names = list(await db.table_names(namespace=["test_ns"]))
        assert "table1" in table_names
        assert "table2" in table_names
        assert len(table_names) == 2

        # Drop one table
        await db.drop_table("table1", namespace=["test_ns"])

        # Verify only table2 remains
        table_names = list(await db.table_names(namespace=["test_ns"]))
        assert "table1" not in table_names
        assert "table2" in table_names
        assert len(table_names) == 1

    async def test_namespace_operations_async(self):
        """Test namespace management operations asynchronously."""
        db = lancedb.connect_namespace_async("dir", {"root": self.temp_dir})

        # Initially no namespaces
        namespaces = await db.list_namespaces()
        assert len(list(namespaces)) == 0

        # Create a namespace
        await db.create_namespace(["test_namespace"])

        # Verify namespace exists
        namespaces = list(await db.list_namespaces())
        assert "test_namespace" in namespaces
        assert len(namespaces) == 1

        # Create table in namespace
        schema = pa.schema(
            [
                pa.field("id", pa.int64()),
                pa.field("vector", pa.list_(pa.float32(), 2)),
            ]
        )
        table = await db.create_table(
            "test_table", schema=schema, namespace=["test_namespace"]
        )
        assert table is not None

        # Verify table exists in namespace
        tables_in_namespace = list(await db.table_names(namespace=["test_namespace"]))
        assert "test_table" in tables_in_namespace
        assert len(tables_in_namespace) == 1

        # Drop table from namespace
        await db.drop_table("test_table", namespace=["test_namespace"])

        # Verify table no longer exists in namespace
        tables_in_namespace = list(await db.table_names(namespace=["test_namespace"]))
        assert len(tables_in_namespace) == 0

        # Drop namespace
        await db.drop_namespace(["test_namespace"])

        # Verify namespace no longer exists
        namespaces = list(await db.list_namespaces())
        assert len(namespaces) == 0

    async def test_drop_all_tables_async(self):
        """Test dropping all tables asynchronously through namespace."""
        db = lancedb.connect_namespace_async("dir", {"root": self.temp_dir})

        # Create a child namespace first
        await db.create_namespace(["test_ns"])

        # Create multiple tables in child namespace
        schema = pa.schema(
            [
                pa.field("id", pa.int64()),
                pa.field("vector", pa.list_(pa.float32(), 2)),
            ]
        )
        for i in range(3):
            await db.create_table(f"table{i}", schema=schema, namespace=["test_ns"])

        # Verify tables exist in child namespace
        table_names = await db.table_names(namespace=["test_ns"])
        assert len(list(table_names)) == 3

        # Drop all tables in child namespace
        await db.drop_all_tables(namespace=["test_ns"])

        # Verify all tables are gone from child namespace
        table_names = await db.table_names(namespace=["test_ns"])
        assert len(list(table_names)) == 0
