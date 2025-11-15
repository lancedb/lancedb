# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

"""Tests for LanceDB namespace integration with DirectoryNamespace."""

import tempfile
import shutil
import pytest
import pyarrow as pa
import lancedb


class TestDirectoryNamespace:
    """Test LanceDB with DirectoryNamespace for comprehensive functionality."""

    def setup_method(self):
        """Set up test fixtures before each test method."""
        self.temp_dir = tempfile.mkdtemp()

    def teardown_method(self):
        """Clean up test fixtures after each test method."""
        if hasattr(self, "temp_dir") and self.temp_dir:
            shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_connect_namespace(self):
        """Test connecting to LanceDB through DirectoryNamespace."""
        # Connect using DirectoryNamespace
        db = lancedb.connect(
            namespace_impl="dir", namespace_properties={"root": self.temp_dir}
        )

        # Should be a LanceDBConnection (wrapping a namespace-based database)
        assert isinstance(db, lancedb.LanceDBConnection)

        # Initially no tables
        assert len(list(db.table_names())) == 0

    def test_create_table_root_namespace(self):
        """Test creating a table in root namespace."""
        db = lancedb.connect(
            namespace_impl="dir", namespace_properties={"root": self.temp_dir}
        )

        # Create table with initial data
        data = pa.Table.from_pydict(
            {
                "id": [1],
                "vector": [[1.0, 2.0]],
                "text": ["test"],
            }
        )

        # Create table in root namespace
        table = db.create_table("test_table", data)
        assert table is not None
        assert table.name == "test_table"

        # Table should appear in root namespace
        table_names = list(db.table_names())
        assert "test_table" in table_names
        assert len(table_names) == 1

        # Verify table has data
        result = table.to_pandas()
        assert len(result) == 1
        assert list(result.columns) == ["id", "vector", "text"]

    def test_create_table_child_namespace(self):
        """Test creating tables in child namespaces using DirectoryNamespace."""
        db = lancedb.connect(
            namespace_impl="dir", namespace_properties={"root": self.temp_dir}
        )

        # Create a child namespace
        db.create_namespace(["test_ns"])

        # Create table in child namespace with initial data
        data = pa.Table.from_pydict(
            {
                "id": [1, 2, 3],
                "vector": [[1.0, 2.0], [3.0, 4.0], [5.0, 6.0]],
                "text": ["a", "b", "c"],
            }
        )

        # Create table in child namespace
        table = db.create_table("test_table", data, namespace=["test_ns"])
        assert table is not None
        assert table.name == "test_table"

        # Verify table exists in child namespace
        tables_in_child = list(db.table_names(namespace=["test_ns"]))
        assert "test_table" in tables_in_child

        # Verify table doesn't exist in root namespace
        tables_in_root = list(db.table_names())
        assert "test_table" not in tables_in_root

        # Open table from child namespace
        opened_table = db.open_table("test_table", namespace=["test_ns"])
        result = opened_table.to_pandas()
        assert len(result) == 3
        assert result["id"].tolist() == [1, 2, 3]

    def test_open_table(self):
        """Test opening an existing table through namespace."""
        db = lancedb.connect(
            namespace_impl="dir", namespace_properties={"root": self.temp_dir}
        )

        # Create a table with data
        data = pa.Table.from_pydict(
            {
                "id": [1],
                "vector": [[1.0, 2.0]],
            }
        )
        db.create_table("test_table", data)

        # Open the table
        table = db.open_table("test_table")
        assert table is not None
        assert table.name == "test_table"

        # Verify table with correct schema
        result = table.to_pandas()
        assert len(result) == 1
        assert list(result.columns) == ["id", "vector"]

    def test_drop_table(self):
        """Test dropping tables through namespace."""
        db = lancedb.connect(
            namespace_impl="dir", namespace_properties={"root": self.temp_dir}
        )

        # Create tables with data
        data = pa.Table.from_pydict(
            {
                "id": [1],
                "vector": [[1.0, 2.0]],
            }
        )
        db.create_table("table1", data)
        db.create_table("table2", data)

        # Verify both tables exist
        table_names = list(db.table_names())
        assert "table1" in table_names
        assert "table2" in table_names
        assert len(table_names) == 2

        # Drop one table
        db.drop_table("table1")

        # Verify only table2 remains
        table_names = list(db.table_names())
        assert "table1" not in table_names
        assert "table2" in table_names
        assert len(table_names) == 1

        # Test that drop_table works without explicit namespace parameter
        db.drop_table("table2")
        assert len(list(db.table_names())) == 0

        # Should not be able to open dropped table
        with pytest.raises(RuntimeError):
            db.open_table("table1")

    def test_create_table_with_data(self):
        """Test creating a table with initial data."""
        db = lancedb.connect(
            namespace_impl="dir", namespace_properties={"root": self.temp_dir}
        )

        # Create table with data
        data = pa.Table.from_pydict(
            {
                "id": [1, 2, 3],
                "vector": [[1.0, 2.0], [3.0, 4.0], [5.0, 6.0]],
            }
        )
        table = db.create_table("test_table", data)

        # Verify data
        result = table.to_pandas()
        assert len(result) == 3
        assert result["id"].tolist() == [1, 2, 3]

    def test_rename_table_not_supported(self):
        """Test that rename_table raises NotImplementedError."""
        db = lancedb.connect(
            namespace_impl="dir", namespace_properties={"root": self.temp_dir}
        )

        data = pa.Table.from_pydict({"id": [1]})
        db.create_table("old_name", data)

        with pytest.raises(NotImplementedError):
            db.rename_table("old_name", "new_name")

    def test_drop_all_tables(self):
        """Test dropping all tables in the namespace."""
        db = lancedb.connect(
            namespace_impl="dir", namespace_properties={"root": self.temp_dir}
        )

        data = pa.Table.from_pydict({"id": [1]})

        # Create multiple tables
        db.create_table("table1", data)
        db.create_table("table2", data)
        db.create_table("table3", data)

        assert len(list(db.table_names())) == 3

        # Drop all tables one by one
        for table_name in list(db.table_names()):
            db.drop_table(table_name)

        assert len(list(db.table_names())) == 0

    def test_table_operations(self):
        """Test various table operations."""
        db = lancedb.connect(
            namespace_impl="dir", namespace_properties={"root": self.temp_dir}
        )

        # Create table with data
        data = pa.Table.from_pydict(
            {
                "id": [1, 2, 3],
                "vector": [[1.0, 2.0], [3.0, 4.0], [5.0, 6.0]],
            }
        )
        table = db.create_table("test_table", data)

        # Add more data
        new_data = pa.Table.from_pydict(
            {
                "id": [4, 5],
                "vector": [[7.0, 8.0], [9.0, 10.0]],
            }
        )
        table.add(new_data)

        # Verify total count
        result = table.to_pandas()
        assert len(result) == 5

    def test_storage_options(self):
        """Test that storage options are passed through."""
        # This test just verifies the connection works with storage options
        db = lancedb.connect(
            namespace_impl="dir", namespace_properties={"root": self.temp_dir}
        )

        # Should be able to create tables normally
        data = pa.Table.from_pydict({"id": [1]})
        table = db.create_table("test_table", data)
        assert table is not None

    def test_namespace_operations(self):
        """Test namespace creation, listing, and dropping."""
        db = lancedb.connect(
            namespace_impl="dir", namespace_properties={"root": self.temp_dir}
        )

        # Initially no namespaces (DirectoryNamespace doesn't list root by default)
        namespaces = list(db.list_namespaces())
        # DirectoryNamespace returns empty list when no child namespaces exist
        assert namespaces == []

        # Create child namespace
        db.create_namespace(["ns1"])

        # List namespaces
        namespaces = list(db.list_namespaces())
        assert "ns1" in namespaces

        # Create nested namespace
        db.create_namespace(["ns1", "child"])

        # List child namespaces under ns1
        child_namespaces = list(db.list_namespaces(namespace=["ns1"]))
        assert "child" in child_namespaces

        # Drop empty namespace
        db.drop_namespace(["ns1", "child"])

        child_namespaces = list(db.list_namespaces(namespace=["ns1"]))
        assert "child" not in child_namespaces

        # Root namespace should still have ns1
        namespaces = list(db.list_namespaces())
        assert "ns1" in namespaces

    def test_namespace_with_tables_cannot_be_dropped(self):
        """Test that namespace with tables cannot be dropped."""
        db = lancedb.connect(
            namespace_impl="dir", namespace_properties={"root": self.temp_dir}
        )

        # Create namespace with a table
        db.create_namespace(["ns1"])
        data = pa.Table.from_pydict({"id": [1]})
        db.create_table("table1", data, namespace=["ns1"])

        # Should not be able to drop namespace with tables
        with pytest.raises(RuntimeError):
            db.drop_namespace(["ns1"])

        # After dropping the table, should be able to drop namespace
        db.drop_table("table1", namespace=["ns1"])
        db.drop_namespace(["ns1"])

        namespaces = list(db.list_namespaces())
        assert "ns1" not in namespaces

    def test_same_table_name_different_namespaces(self):
        """Test creating tables with same name in different namespaces."""
        db = lancedb.connect(
            namespace_impl="dir", namespace_properties={"root": self.temp_dir}
        )

        # Create two namespaces
        db.create_namespace(["namespace_a"])
        db.create_namespace(["namespace_b"])

        # Create table with same name in both namespaces with initial data
        initial_data = pa.Table.from_pydict(
            {
                "id": [0],
                "vector": [[0.0, 0.0]],
                "text": ["init"],
            }
        )

        # Create table with same name in both namespaces
        table_a = db.create_table(
            "same_name_table", initial_data, namespace=["namespace_a"]
        )
        table_b = db.create_table(
            "same_name_table", initial_data, namespace=["namespace_b"]
        )

        # Add different data to each
        data_a = pa.Table.from_pydict(
            {
                "id": [1, 2],
                "vector": [[1.0, 2.0], [3.0, 4.0]],
                "text": ["data_from_namespace_a", "also_from_namespace_a"],
            }
        )
        table_a.add(data_a)

        data_b = pa.Table.from_pydict(
            {
                "id": [10, 20, 30],
                "vector": [[10.0, 20.0], [30.0, 40.0], [50.0, 60.0]],
                "text": [
                    "data_from_namespace_b",
                    "also_from_namespace_b",
                    "more_from_namespace_b",
                ],
            }
        )
        table_b.add(data_b)

        # Verify data in namespace_a table
        opened_table_a = db.open_table("same_name_table", namespace=["namespace_a"])
        result_a = opened_table_a.to_pandas().sort_values("id").reset_index(drop=True)
        assert len(result_a) == 3  # 1 initial + 2 added
        assert result_a["id"].tolist() == [0, 1, 2]
        assert result_a["text"].tolist() == [
            "init",
            "data_from_namespace_a",
            "also_from_namespace_a",
        ]
        assert [v.tolist() for v in result_a["vector"]] == [
            [0.0, 0.0],
            [1.0, 2.0],
            [3.0, 4.0],
        ]

        # Verify data in namespace_b table
        opened_table_b = db.open_table("same_name_table", namespace=["namespace_b"])
        result_b = opened_table_b.to_pandas().sort_values("id").reset_index(drop=True)
        assert len(result_b) == 4  # 1 initial + 3 added
        assert result_b["id"].tolist() == [0, 10, 20, 30]
        assert result_b["text"].tolist() == [
            "init",
            "data_from_namespace_b",
            "also_from_namespace_b",
            "more_from_namespace_b",
        ]
        assert [v.tolist() for v in result_b["vector"]] == [
            [0.0, 0.0],
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

    def test_multi_level_namespaces(self):
        """Test multi-level child namespaces with DirectoryNamespace."""
        db = lancedb.connect(
            namespace_impl="dir", namespace_properties={"root": self.temp_dir}
        )

        # Create multi-level namespace
        db.create_namespace(["level1"])
        db.create_namespace(["level1", "level2"])

        # Create table in level1 with initial data
        data1 = pa.Table.from_pydict({"id": [1], "value": ["level1_data"]})
        db.create_table("table1", data1, namespace=["level1"])

        # Create table in level1/level2 with initial data
        data2 = pa.Table.from_pydict({"id": [2], "value": ["level2_data"]})
        db.create_table("table2", data2, namespace=["level1", "level2"])

        # Verify tables in correct namespaces
        level1_tables = list(db.table_names(namespace=["level1"]))
        assert "table1" in level1_tables
        assert "table2" not in level1_tables

        level2_tables = list(db.table_names(namespace=["level1", "level2"]))
        assert "table2" in level2_tables
        assert "table1" not in level2_tables

        # Verify data
        opened1 = db.open_table("table1", namespace=["level1"])
        result1 = opened1.to_pandas()
        assert result1["value"][0] == "level1_data"

        opened2 = db.open_table("table2", namespace=["level1", "level2"])
        result2 = opened2.to_pandas()
        assert result2["value"][0] == "level2_data"

    def test_drop_operations_child_namespace(self):
        """Test drop operations in child namespaces."""
        db = lancedb.connect(
            namespace_impl="dir", namespace_properties={"root": self.temp_dir}
        )

        # Create child namespace with tables
        db.create_namespace(["drop_test"])

        data = pa.Table.from_pydict({"id": [1]})

        db.create_table("table1", data, namespace=["drop_test"])
        db.create_table("table2", data, namespace=["drop_test"])

        # Verify both tables exist
        tables = list(db.table_names(namespace=["drop_test"]))
        assert len(tables) == 2
        assert "table1" in tables
        assert "table2" in tables

        # Drop one table
        db.drop_table("table1", namespace=["drop_test"])

        # Verify only one table remains
        tables = list(db.table_names(namespace=["drop_test"]))
        assert len(tables) == 1
        assert "table2" in tables
        assert "table1" not in tables

    def test_isolated_namespaces(self):
        """Test that different namespaces are properly isolated."""
        db = lancedb.connect(
            namespace_impl="dir", namespace_properties={"root": self.temp_dir}
        )

        # Create two separate child namespaces
        db.create_namespace(["ns_a"])
        db.create_namespace(["ns_b"])

        # Create same table name in both namespaces with different data
        data_a = pa.Table.from_pydict({"id": [1], "data": ["from_ns_a"]})
        db.create_table("shared_name", data_a, namespace=["ns_a"])

        data_b = pa.Table.from_pydict({"id": [2], "data": ["from_ns_b"]})
        db.create_table("shared_name", data_b, namespace=["ns_b"])

        # Verify isolation
        opened_a = db.open_table("shared_name", namespace=["ns_a"])
        result_a = opened_a.to_pandas()
        assert result_a["data"][0] == "from_ns_a"

        opened_b = db.open_table("shared_name", namespace=["ns_b"])
        result_b = opened_b.to_pandas()
        assert result_b["data"][0] == "from_ns_b"
