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
    ListNamespacesRequest,
    ListNamespacesResponse,
    CreateNamespaceRequest,
    CreateNamespaceResponse,
    DropNamespaceRequest,
    DropNamespaceResponse,
)


class TempNamespace(LanceNamespace):
    """A simple dictionary-backed namespace for testing."""

    # Class-level storage to persist table registry across instances
    _global_registry: Dict[str, Dict[str, str]] = {}
    # Class-level storage for namespaces (supporting 1-level namespace)
    _global_namespaces: Dict[str, set] = {}

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
        if root not in self._global_namespaces:
            self._global_namespaces[root] = set()
        self.tables = self._global_registry[root]  # Reference to shared registry
        self.namespaces = self._global_namespaces[
            root
        ]  # Reference to shared namespaces

    def list_tables(self, request: ListTablesRequest) -> ListTablesResponse:
        """List all tables in the namespace."""
        if request.id is None:
            # List all tables in root namespace
            tables = [name for name in self.tables.keys() if "." not in name]
        else:
            # List tables in specific namespace (1-level only)
            if len(request.id) == 1:
                namespace_name = request.id[0]
                prefix = f"{namespace_name}."
                tables = [
                    name[len(prefix) :]
                    for name in self.tables.keys()
                    if name.startswith(prefix)
                ]
            else:
                # Multi-level namespaces not supported
                raise ValueError("Only 1-level namespaces are supported")
        return ListTablesResponse(tables=tables)

    def describe_table(self, request: DescribeTableRequest) -> DescribeTableResponse:
        """Describe a table by returning its location."""
        if not request.id:
            raise ValueError("Invalid table ID")

        if len(request.id) == 1:
            # Root namespace table
            table_name = request.id[0]
        elif len(request.id) == 2:
            # Namespaced table (1-level namespace)
            namespace_name, table_name = request.id
            table_name = f"{namespace_name}.{table_name}"
        else:
            raise ValueError("Only 1-level namespaces are supported")

        if table_name not in self.tables:
            raise RuntimeError(f"Table does not exist: {table_name}")

        table_uri = self.tables[table_name]
        return DescribeTableResponse(location=table_uri)

    def create_table(
        self, request: CreateTableRequest, request_data: bytes
    ) -> CreateTableResponse:
        """Create a table in the namespace."""
        if not request.id:
            raise ValueError("Invalid table ID")

        if len(request.id) == 1:
            # Root namespace table
            table_name = request.id[0]
            table_uri = f"{self.config.root}/{table_name}.lance"
        elif len(request.id) == 2:
            # Namespaced table (1-level namespace)
            namespace_name, base_table_name = request.id
            # Add namespace to our namespace set
            self.namespaces.add(namespace_name)
            table_name = f"{namespace_name}.{base_table_name}"
            table_uri = f"{self.config.root}/{namespace_name}/{base_table_name}.lance"
        else:
            raise ValueError("Only 1-level namespaces are supported")

        # Check if table already exists
        if table_name in self.tables:
            if request.mode == "overwrite":
                # Drop existing table for overwrite mode
                del self.tables[table_name]
            else:
                raise RuntimeError(f"Table already exists: {table_name}")

        # Parse the Arrow IPC stream to get the schema and create the actual table
        import pyarrow.ipc as ipc
        import io
        import lance
        import os

        # Create directory if needed for namespaced tables
        os.makedirs(os.path.dirname(table_uri), exist_ok=True)

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
        if not request.id:
            raise ValueError("Invalid table ID")

        if len(request.id) == 1:
            # Root namespace table
            table_name = request.id[0]
        elif len(request.id) == 2:
            # Namespaced table (1-level namespace)
            namespace_name, base_table_name = request.id
            table_name = f"{namespace_name}.{base_table_name}"
        else:
            raise ValueError("Only 1-level namespaces are supported")

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

    def list_namespaces(self, request: ListNamespacesRequest) -> ListNamespacesResponse:
        """List child namespaces."""
        if request.id is None:
            # List root-level namespaces
            namespaces = list(self.namespaces)
        elif len(request.id) == 1:
            # For 1-level namespace, there are no child namespaces
            namespaces = []
        else:
            raise ValueError("Only 1-level namespaces are supported")

        return ListNamespacesResponse(namespaces=namespaces)

    def create_namespace(
        self, request: CreateNamespaceRequest
    ) -> CreateNamespaceResponse:
        """Create a namespace."""
        if not request.id:
            raise ValueError("Invalid namespace ID")

        if len(request.id) == 1:
            # Create 1-level namespace
            namespace_name = request.id[0]
            self.namespaces.add(namespace_name)

            # Create directory for the namespace
            import os

            namespace_dir = f"{self.config.root}/{namespace_name}"
            os.makedirs(namespace_dir, exist_ok=True)
        else:
            raise ValueError("Only 1-level namespaces are supported")

        return CreateNamespaceResponse()

    def drop_namespace(self, request: DropNamespaceRequest) -> DropNamespaceResponse:
        """Drop a namespace."""
        if not request.id:
            raise ValueError("Invalid namespace ID")

        if len(request.id) == 1:
            # Drop 1-level namespace
            namespace_name = request.id[0]

            if namespace_name not in self.namespaces:
                raise RuntimeError(f"Namespace does not exist: {namespace_name}")

            # Check if namespace has any tables
            prefix = f"{namespace_name}."
            tables_in_namespace = [
                name for name in self.tables.keys() if name.startswith(prefix)
            ]
            if tables_in_namespace:
                raise RuntimeError(
                    f"Cannot drop namespace '{namespace_name}': contains tables"
                )

            # Remove namespace
            self.namespaces.remove(namespace_name)

            # Remove directory
            import shutil
            import os

            namespace_dir = f"{self.config.root}/{namespace_name}"
            if os.path.exists(namespace_dir):
                shutil.rmtree(namespace_dir, ignore_errors=True)
        else:
            raise ValueError("Only 1-level namespaces are supported")

        return DropNamespaceResponse()


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
        if self.temp_dir in TempNamespace._global_namespaces:
            TempNamespace._global_namespaces[self.temp_dir].clear()

    def teardown_method(self):
        """Clean up test fixtures."""
        # Clear the TestNamespace registry
        if self.temp_dir in TempNamespace._global_registry:
            del TempNamespace._global_registry[self.temp_dir]
        if self.temp_dir in TempNamespace._global_namespaces:
            del TempNamespace._global_namespaces[self.temp_dir]
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

    def test_namespace_operations(self):
        """Test namespace management operations."""
        db = lancedb.connect_namespace("temp", {"root": self.temp_dir})

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
        db = lancedb.connect_namespace("temp", {"root": self.temp_dir})

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
        with pytest.raises(RuntimeError, match="contains tables"):
            db.drop_namespace(["test_namespace"])

        # Drop table first
        db.drop_table("test_table", namespace=["test_namespace"])

        # Now dropping namespace should work
        db.drop_namespace(["test_namespace"])
