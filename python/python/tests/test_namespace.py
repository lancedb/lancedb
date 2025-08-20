# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

"""Tests for LanceDB namespace integration."""

import tempfile
import shutil
from typing import Dict, Optional
import pytest
import pyarrow as pa
import lancedb
from lance_namespace.namespace import NATIVE_IMPLS, LanceNamespace
from lance_namespace_urllib3_client.models import (
    ListTablesRequest,
    ListTablesResponse,
    DescribeTableRequest,
    DescribeTableResponse,
    RegisterTableRequest,
    RegisterTableResponse,
    DeregisterTableRequest,
    DeregisterTableResponse,
    CreateTableRequest,
    CreateTableResponse,
    DropTableRequest,
    DropTableResponse,
)


class TempNamespace(LanceNamespace):
    """A simple dictionary-backed namespace for testing."""

    # Class-level storage to persist table registry across instances
    _global_registry: Dict[str, Dict[str, str]] = {}

    def __init__(self, **properties):
        """Initialize the test namespace.

        Args:
            root: The root directory for tables (optional)
            **properties: Additional configuration properties
        """
        self.config = TempNamespaceConfig(properties)
        # Use the root as a key to maintain separate registries per root
        root = self.config.root
        if root not in self._global_registry:
            self._global_registry[root] = {}
        self.tables = self._global_registry[root]  # Reference to shared registry

    def list_tables(self, request: ListTablesRequest) -> ListTablesResponse:
        """List all tables in the namespace."""
        # For simplicity, ignore namespace ID validation
        tables = list(self.tables.keys())
        return ListTablesResponse(tables=tables)

    def describe_table(self, request: DescribeTableRequest) -> DescribeTableResponse:
        """Describe a table by returning its location."""
        if not request.id or len(request.id) != 1:
            raise ValueError("Invalid table ID")

        table_name = request.id[0]
        if table_name not in self.tables:
            raise RuntimeError(f"Table does not exist: {table_name}")

        table_uri = self.tables[table_name]
        return DescribeTableResponse(location=table_uri)

    def create_table(
        self, request: CreateTableRequest, request_data: bytes
    ) -> CreateTableResponse:
        """Create a table in the namespace."""
        if not request.id or len(request.id) != 1:
            raise ValueError("Invalid table ID")

        table_name = request.id[0]

        # Check if table already exists
        if table_name in self.tables:
            if request.mode == "overwrite":
                # Drop existing table for overwrite mode
                del self.tables[table_name]
            else:
                raise RuntimeError(f"Table already exists: {table_name}")

        # Generate table URI based on root directory
        table_uri = f"{self.config.root}/{table_name}.lance"

        # Parse the Arrow IPC stream to get the schema and create the actual table
        import pyarrow.ipc as ipc
        import io
        import lance

        # Read the IPC stream
        reader = ipc.open_stream(io.BytesIO(request_data))
        table = reader.read_all()

        # Create the actual Lance table
        lance.write_dataset(table, table_uri)

        # Store the table mapping
        self.tables[table_name] = table_uri

        return CreateTableResponse(location=table_uri)

    def drop_table(self, request: DropTableRequest) -> DropTableResponse:
        """Drop a table from the namespace."""
        if not request.id or len(request.id) != 1:
            raise ValueError("Invalid table ID")

        table_name = request.id[0]
        if table_name not in self.tables:
            raise RuntimeError(f"Table does not exist: {table_name}")

        # Get the table URI
        table_uri = self.tables[table_name]

        # Delete the actual table files
        import shutil
        import os

        if os.path.exists(table_uri):
            shutil.rmtree(table_uri, ignore_errors=True)

        # Remove from registry
        del self.tables[table_name]

        return DropTableResponse()

    def register_table(self, request: RegisterTableRequest) -> RegisterTableResponse:
        """Register a table with the namespace."""
        if not request.id or len(request.id) != 1:
            raise ValueError("Invalid table ID")

        if not request.location:
            raise ValueError("Table location is required")

        table_name = request.id[0]
        self.tables[table_name] = request.location

        return RegisterTableResponse()

    def deregister_table(
        self, request: DeregisterTableRequest
    ) -> DeregisterTableResponse:
        """Deregister a table from the namespace."""
        if not request.id or len(request.id) != 1:
            raise ValueError("Invalid table ID")

        table_name = request.id[0]
        if table_name not in self.tables:
            raise RuntimeError(f"Table does not exist: {table_name}")

        del self.tables[table_name]
        return DeregisterTableResponse()


class TempNamespaceConfig:
    """Configuration for TestNamespace."""

    ROOT = "root"

    def __init__(self, properties: Optional[Dict[str, str]] = None):
        """Initialize configuration from properties.

        Args:
            properties: Dictionary of configuration properties
        """
        if properties is None:
            properties = {}

        self._root = properties.get(self.ROOT, "/tmp")

    @property
    def root(self) -> str:
        """Get the namespace root directory."""
        return self._root


NATIVE_IMPLS["temp"] = f"{TempNamespace.__module__}.TempNamespace"


class TestNamespaceConnection:
    """Test namespace-based LanceDB connection."""

    def setup_method(self):
        """Set up test fixtures."""
        self.temp_dir = tempfile.mkdtemp()
        # Clear the TestNamespace registry for this test
        if self.temp_dir in TempNamespace._global_registry:
            TempNamespace._global_registry[self.temp_dir].clear()

    def teardown_method(self):
        """Clean up test fixtures."""
        # Clear the TestNamespace registry
        if self.temp_dir in TempNamespace._global_registry:
            del TempNamespace._global_registry[self.temp_dir]
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_connect_namespace_test(self):
        """Test connecting to LanceDB through TestNamespace."""
        # Connect using TestNamespace
        db = lancedb.connect_namespace("temp", {"root": self.temp_dir})

        # Should be a LanceNamespaceDBConnection
        assert isinstance(db, lancedb.LanceNamespaceDBConnection)

        # Initially no tables
        assert len(list(db.table_names())) == 0

    def test_create_table_through_namespace(self):
        """Test creating a table through namespace."""
        db = lancedb.connect_namespace("temp", {"root": self.temp_dir})

        # Define schema for empty table
        schema = pa.schema(
            [
                pa.field("id", pa.int64()),
                pa.field("vector", pa.list_(pa.float32(), 2)),
                pa.field("text", pa.string()),
            ]
        )

        # Create empty table
        table = db.create_table("test_table", schema=schema)
        assert table is not None
        assert table.name == "test_table"

        # Table should appear in namespace
        table_names = list(db.table_names())
        assert "test_table" in table_names
        assert len(table_names) == 1

        # Verify empty table
        result = table.to_pandas()
        assert len(result) == 0
        assert list(result.columns) == ["id", "vector", "text"]

    def test_open_table_through_namespace(self):
        """Test opening an existing table through namespace."""
        db = lancedb.connect_namespace("temp", {"root": self.temp_dir})

        # Create a table with schema
        schema = pa.schema(
            [
                pa.field("id", pa.int64()),
                pa.field("vector", pa.list_(pa.float32(), 2)),
            ]
        )
        db.create_table("test_table", schema=schema)

        # Open the table
        table = db.open_table("test_table")
        assert table is not None
        assert table.name == "test_table"

        # Verify empty table with correct schema
        result = table.to_pandas()
        assert len(result) == 0
        assert list(result.columns) == ["id", "vector"]

    def test_drop_table_through_namespace(self):
        """Test dropping a table through namespace."""
        db = lancedb.connect_namespace("temp", {"root": self.temp_dir})

        # Create tables
        schema = pa.schema(
            [
                pa.field("id", pa.int64()),
                pa.field("vector", pa.list_(pa.float32(), 2)),
            ]
        )
        db.create_table("table1", schema=schema)
        db.create_table("table2", schema=schema)

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

        # Should not be able to open dropped table
        with pytest.raises(RuntimeError):
            db.open_table("table1")

    def test_create_table_with_schema(self):
        """Test creating a table with explicit schema through namespace."""
        db = lancedb.connect_namespace("temp", {"root": self.temp_dir})

        # Define schema
        schema = pa.schema(
            [
                pa.field("id", pa.int64()),
                pa.field("vector", pa.list_(pa.float32(), 3)),
                pa.field("text", pa.string()),
            ]
        )

        # Create table with schema
        table = db.create_table("test_table", schema=schema)
        assert table is not None

        # Verify schema
        table_schema = table.schema
        assert len(table_schema) == 3
        assert table_schema.field("id").type == pa.int64()
        assert table_schema.field("text").type == pa.string()

    def test_rename_table_not_supported(self):
        """Test that rename_table raises NotImplementedError."""
        db = lancedb.connect_namespace("temp", {"root": self.temp_dir})

        # Create a table
        schema = pa.schema(
            [
                pa.field("id", pa.int64()),
                pa.field("vector", pa.list_(pa.float32(), 2)),
            ]
        )
        db.create_table("old_name", schema=schema)

        # Rename should raise NotImplementedError
        with pytest.raises(NotImplementedError, match="rename_table is not supported"):
            db.rename_table("old_name", "new_name")

    def test_drop_all_tables(self):
        """Test dropping all tables through namespace."""
        db = lancedb.connect_namespace("temp", {"root": self.temp_dir})

        # Create multiple tables
        schema = pa.schema(
            [
                pa.field("id", pa.int64()),
                pa.field("vector", pa.list_(pa.float32(), 2)),
            ]
        )
        for i in range(3):
            db.create_table(f"table{i}", schema=schema)

        # Verify tables exist
        assert len(list(db.table_names())) == 3

        # Drop all tables
        db.drop_all_tables()

        # Verify all tables are gone
        assert len(list(db.table_names())) == 0

    def test_table_operations(self):
        """Test various table operations through namespace."""
        db = lancedb.connect_namespace("temp", {"root": self.temp_dir})

        # Create a table with schema
        schema = pa.schema(
            [
                pa.field("id", pa.int64()),
                pa.field("vector", pa.list_(pa.float32(), 2)),
                pa.field("text", pa.string()),
            ]
        )
        table = db.create_table("test_table", schema=schema)

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
            "temp", {"root": self.temp_dir}, storage_options=storage_opts
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
