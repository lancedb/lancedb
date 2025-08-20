# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

"""
LanceDB Namespace integration module.

This module provides integration with lance_namespace for managing tables
through a namespace abstraction.
"""

from __future__ import annotations

from typing import Dict, Iterable, List, Optional, Union, override
import os

from lancedb.db import DBConnection
from lancedb.table import LanceTable, Table
from lancedb.util import validate_table_name
from lancedb.common import validate_schema
from lancedb.table import sanitize_create_table

from lance_namespace import LanceNamespace, connect as namespace_connect
from lance_namespace_urllib3_client.models import (
    ListTablesRequest,
    DescribeTableRequest,
    CreateTableRequest,
    DropTableRequest,
    JsonArrowSchema,
    JsonArrowField,
    JsonArrowDataType,
)

import pyarrow as pa
from datetime import timedelta
from lancedb.pydantic import LanceModel
from lancedb.common import DATA
from lancedb.embeddings import EmbeddingFunctionConfig
from ._lancedb import Session


def _convert_pyarrow_type_to_json(arrow_type: pa.DataType) -> JsonArrowDataType:
    """Convert PyArrow DataType to JsonArrowDataType."""
    if pa.types.is_null(arrow_type):
        type_name = "null"
    elif pa.types.is_boolean(arrow_type):
        type_name = "bool"
    elif pa.types.is_int8(arrow_type):
        type_name = "int8"
    elif pa.types.is_uint8(arrow_type):
        type_name = "uint8"
    elif pa.types.is_int16(arrow_type):
        type_name = "int16"
    elif pa.types.is_uint16(arrow_type):
        type_name = "uint16"
    elif pa.types.is_int32(arrow_type):
        type_name = "int32"
    elif pa.types.is_uint32(arrow_type):
        type_name = "uint32"
    elif pa.types.is_int64(arrow_type):
        type_name = "int64"
    elif pa.types.is_uint64(arrow_type):
        type_name = "uint64"
    elif pa.types.is_float32(arrow_type):
        type_name = "float32"
    elif pa.types.is_float64(arrow_type):
        type_name = "float64"
    elif pa.types.is_string(arrow_type):
        type_name = "utf8"
    elif pa.types.is_binary(arrow_type):
        type_name = "binary"
    elif pa.types.is_list(arrow_type):
        # For list types, we need more complex handling
        type_name = "list"
    elif pa.types.is_fixed_size_list(arrow_type):
        type_name = "fixed_size_list"
    else:
        # Default to string representation for unsupported types
        type_name = str(arrow_type)

    return JsonArrowDataType(type=type_name)


def _convert_pyarrow_schema_to_json(schema: pa.Schema) -> JsonArrowSchema:
    """Convert PyArrow Schema to JsonArrowSchema."""
    fields = []
    for field in schema:
        json_field = JsonArrowField(
            name=field.name,
            type=_convert_pyarrow_type_to_json(field.type),
            nullable=field.nullable,
            metadata=field.metadata,
        )
        fields.append(json_field)

    return JsonArrowSchema(fields=fields, metadata=schema.metadata)


class LanceNamespaceDBConnection(DBConnection):
    """
    A LanceDB connection that uses a namespace for table management.

    This connection delegates table URI resolution to a lance_namespace instance,
    while using the standard LanceTable for actual table operations.
    """

    def __init__(
        self,
        namespace: LanceNamespace,
        *,
        read_consistency_interval: Optional[timedelta] = None,
        storage_options: Optional[Dict[str, str]] = None,
        session: Optional[Session] = None,
    ):
        """
        Initialize a namespace-based LanceDB connection.

        Parameters
        ----------
        namespace : LanceNamespace
            The namespace instance to use for table management
        read_consistency_interval : Optional[timedelta]
            The interval at which to check for updates to the table from other
            processes. If None, then consistency is not checked.
        storage_options : Optional[Dict[str, str]]
            Additional options for the storage backend
        session : Optional[Session]
            A session to use for this connection
        """
        self._ns = namespace
        self.read_consistency_interval = read_consistency_interval
        self.storage_options = storage_options or {}
        self.session = session

    @override
    def table_names(
        self, page_token: Optional[str] = None, limit: int = 10
    ) -> Iterable[str]:
        # Use namespace to list tables
        request = ListTablesRequest(id=None, page_token=page_token, limit=limit)
        response = self._ns.list_tables(request)
        return response.tables if response.tables else []

    @override
    def create_table(
        self,
        name: str,
        data: Optional[DATA] = None,
        schema: Optional[Union[pa.Schema, LanceModel]] = None,
        mode: str = "create",
        exist_ok: bool = False,
        on_bad_vectors: str = "error",
        fill_value: float = 0.0,
        embedding_functions: Optional[List[EmbeddingFunctionConfig]] = None,
        *,
        storage_options: Optional[Dict[str, str]] = None,
        data_storage_version: Optional[str] = None,
        enable_v2_manifest_paths: Optional[bool] = None,
    ) -> Table:
        if mode.lower() not in ["create", "overwrite"]:
            raise ValueError("mode must be either 'create' or 'overwrite'")
        validate_table_name(name)

        # TODO: support passing data
        if data is not None:
            raise ValueError(
                "create_table currently only supports creating empty tables (data=None)"
            )

        # Prepare schema
        metadata = None
        if embedding_functions is not None:
            from lancedb.embeddings.registry import EmbeddingFunctionRegistry

            registry = EmbeddingFunctionRegistry.get_instance()
            metadata = registry.get_table_metadata(embedding_functions)

        data, schema = sanitize_create_table(
            data, schema, metadata, on_bad_vectors, fill_value
        )
        validate_schema(schema)

        # Convert PyArrow schema to JsonArrowSchema
        json_schema = _convert_pyarrow_schema_to_json(schema)

        # Create table request
        request = CreateTableRequest(id=[name], var_schema=json_schema)

        # Create empty Arrow IPC stream bytes
        import pyarrow.ipc as ipc
        import io

        empty_table = pa.Table.from_arrays(
            [pa.array([], type=field.type) for field in schema], schema=schema
        )
        buffer = io.BytesIO()
        with ipc.new_stream(buffer, schema) as writer:
            writer.write_table(empty_table)
        request_data = buffer.getvalue()

        self._ns.create_table(request, request_data)
        return self.open_table(name, storage_options=storage_options)

    @override
    def open_table(
        self,
        name: str,
        *,
        storage_options: Optional[Dict[str, str]] = None,
        index_cache_size: Optional[int] = None,
    ) -> Table:
        request = DescribeTableRequest(id=[name])
        response = self._ns.describe_table(request)

        merged_storage_options = dict()
        if storage_options:
            merged_storage_options.update(storage_options)
        if response.storage_options:
            merged_storage_options.update(response.storage_options)

        return self._lance_table_from_uri(
            response.location,
            storage_options=merged_storage_options,
            index_cache_size=index_cache_size,
        )

    @override
    def drop_table(self, name: str):
        # Use namespace drop_table directly
        request = DropTableRequest(id=[name])
        self._ns.drop_table(request)

    @override
    def rename_table(self, cur_name: str, new_name: str):
        raise NotImplementedError(
            "rename_table is not supported for namespace connections"
        )

    @override
    def drop_database(self):
        raise NotImplementedError(
            "drop_database is deprecated, use drop_all_tables instead"
        )

    @override
    def drop_all_tables(self):
        for table_name in self.table_names():
            self.drop_table(table_name)

    def _lance_table_from_uri(
        self,
        table_uri: str,
        *,
        storage_options: Optional[Dict[str, str]] = None,
        index_cache_size: Optional[int] = None,
    ) -> LanceTable:
        # Extract the base path and table name from the URI
        if table_uri.endswith(".lance"):
            base_path = os.path.dirname(table_uri)
            table_name = os.path.basename(table_uri)[:-6]  # Remove .lance
        else:
            raise ValueError(f"Invalid table URI: {table_uri}")

        from lancedb.db import LanceDBConnection

        temp_conn = LanceDBConnection(
            base_path,
            read_consistency_interval=self.read_consistency_interval,
            storage_options={**self.storage_options, **(storage_options or {})},
            session=self.session,
        )

        # Open the table using the temporary connection
        return LanceTable.open(
            temp_conn,
            table_name,
            storage_options=storage_options,
            index_cache_size=index_cache_size,
        )


def connect_namespace(
    impl: str,
    properties: Dict[str, str],
    *,
    read_consistency_interval: Optional[timedelta] = None,
    storage_options: Optional[Dict[str, str]] = None,
    session: Optional[Session] = None,
) -> LanceNamespaceDBConnection:
    """
    Connect to a LanceDB database through a namespace.

    Parameters
    ----------
    impl : str
        The namespace implementation to use. For examples:
        - "dir" for DirectoryNamespace
        - "rest" for REST-based namespace
        - Full module path for custom implementations
    properties : Dict[str, str]
        Configuration properties for the namespace implementation.
        Different namespace implemenation has different config properties.
        For example, use DirectoryNamespace with {"root": "/path/to/directory"}
    read_consistency_interval : Optional[timedelta]
        The interval at which to check for updates to the table from other
        processes. If None, then consistency is not checked.
    storage_options : Optional[Dict[str, str]]
        Additional options for the storage backend
    session : Optional[Session]
        A session to use for this connection

    Returns
    -------
    LanceNamespaceDBConnection
        A namespace-based connection to LanceDB

    Examples
    --------
    >>> import lancedb
    >>> import pyarrow as pa
    >>> # Connect using GlueNamespace
    >>> db = lancedb.connect_namespace("glue", {"catalog_id": "123456789012"})
    >>> # Create a table with schema
    >>> schema = pa.schema([
    ...     pa.field("id", pa.int64()),
    ...     pa.field("vector", pa.list_(pa.float32(), 2))
    ... ])
    >>> table = db.create_table("my_table", schema=schema)
    >>> # List tables
    >>> db.table_names()
    ['my_table']
    """
    namespace = namespace_connect(impl, properties)

    # Return the namespace-based connection
    return LanceNamespaceDBConnection(
        namespace,
        read_consistency_interval=read_consistency_interval,
        storage_options=storage_options,
        session=session,
    )
