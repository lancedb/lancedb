# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

"""
LanceDB Namespace integration module.

This module provides integration with lance_namespace for managing tables
through a namespace abstraction.
"""

from __future__ import annotations

import asyncio
import sys
from typing import Dict, Iterable, List, Optional, Union

if sys.version_info >= (3, 12):
    from typing import override
else:
    from overrides import override

from datetime import timedelta
import pyarrow as pa

from lancedb.db import DBConnection, LanceDBConnection
from lancedb.namespace_utils import (
    _normalize_create_namespace_mode,
    _normalize_drop_namespace_mode,
    _normalize_drop_namespace_behavior,
)
from lancedb.io import StorageOptionsProvider
from lance_namespace import (
    LanceNamespace,
    connect as namespace_connect,
    CreateNamespaceResponse,
    DescribeNamespaceResponse,
    DropNamespaceResponse,
    ListNamespacesResponse,
    ListTablesResponse,
    ListTablesRequest,
    DescribeTableRequest,
    DescribeNamespaceRequest,
    DropTableRequest,
    ListNamespacesRequest,
    CreateNamespaceRequest,
    DropNamespaceRequest,
    CreateEmptyTableRequest,
)
from lancedb.table import AsyncTable, LanceTable, Table
from lancedb.util import validate_table_name
from lancedb.common import DATA
from lancedb.pydantic import LanceModel
from lancedb.embeddings import EmbeddingFunctionConfig
from ._lancedb import Session

from lance_namespace_urllib3_client.models.json_arrow_schema import JsonArrowSchema
from lance_namespace_urllib3_client.models.json_arrow_field import JsonArrowField
from lance_namespace_urllib3_client.models.json_arrow_data_type import JsonArrowDataType


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

    # decode binary metadata to strings for JSON
    meta = None
    if schema.metadata:
        meta = {
            k.decode("utf-8"): v.decode("utf-8") for k, v in schema.metadata.items()
        }

    return JsonArrowSchema(fields=fields, metadata=meta)


class LanceNamespaceStorageOptionsProvider(StorageOptionsProvider):
    """Storage options provider that fetches storage options from a LanceNamespace.

    This provider automatically fetches fresh storage options by calling the
    namespace's describe_table() method, which returns both the table location
    and time-limited storage options. This enables automatic credential refresh
    for tables accessed through namespace connections.

    Parameters
    ----------
    namespace : LanceNamespace
        The namespace instance with a describe_table() method
    table_id : List[str]
        The table identifier (namespace path + table name)

    Examples
    --------
    Create a provider and fetch storage options::

        from lance_namespace import connect as namespace_connect

        # Connect to namespace (requires a running namespace server)
        namespace = namespace_connect("rest", {"uri": "https://..."})
        provider = LanceNamespaceStorageOptionsProvider(
            namespace=namespace,
            table_id=["my_namespace", "my_table"]
        )
        options = provider.fetch_storage_options()
    """

    def __init__(self, namespace: LanceNamespace, table_id: List[str]):
        """Initialize with namespace and table ID.

        Parameters
        ----------
        namespace : LanceNamespace
            The namespace instance with a describe_table() method
        table_id : List[str]
            The table identifier
        """
        self._namespace = namespace
        self._table_id = table_id

    def fetch_storage_options(self) -> Dict[str, str]:
        """Fetch storage options from the namespace.

        This calls namespace.describe_table() to get the latest storage options
        and their expiration time.

        Returns
        -------
        Dict[str, str]
            Flat dictionary of string key-value pairs containing storage options.
            May include "expires_at_millis" key for automatic refresh.

        Raises
        ------
        RuntimeError
            If namespace does not return storage_options
        """
        request = DescribeTableRequest(id=self._table_id, version=None)
        response = self._namespace.describe_table(request)
        storage_options = response.storage_options
        if storage_options is None:
            raise RuntimeError(
                "Namespace did not return storage_options. "
                "Ensure the namespace supports storage options providing."
            )

        # Return the storage_options directly - it's already a flat Map<String, String>
        return storage_options

    def provider_id(self) -> str:
        """Return a human-readable unique identifier for this provider instance."""
        # Try to call namespace_id() if available (lance-namespace >= 0.0.20)
        if hasattr(self._namespace, "namespace_id"):
            namespace_id = self._namespace.namespace_id()
        else:
            # Fallback for older namespace versions
            namespace_id = str(self._namespace)

        return (
            f"LanceNamespaceStorageOptionsProvider {{ "
            f"namespace: {namespace_id}, table_id: {self._table_id!r} }}"
        )


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
        self,
        page_token: Optional[str] = None,
        limit: int = 10,
        *,
        namespace: Optional[List[str]] = None,
    ) -> Iterable[str]:
        """
        List table names in the database.

        .. deprecated::
            Use :meth:`list_tables` instead, which provides proper pagination support.
        """
        import warnings

        warnings.warn(
            "table_names() is deprecated, use list_tables() instead",
            DeprecationWarning,
            stacklevel=2,
        )
        if namespace is None:
            namespace = []
        request = ListTablesRequest(id=namespace, page_token=page_token, limit=limit)
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
        namespace: Optional[List[str]] = None,
        storage_options: Optional[Dict[str, str]] = None,
        storage_options_provider: Optional[StorageOptionsProvider] = None,
        data_storage_version: Optional[str] = None,
        enable_v2_manifest_paths: Optional[bool] = None,
    ) -> Table:
        if namespace is None:
            namespace = []
        if mode.lower() not in ["create", "overwrite"]:
            raise ValueError("mode must be either 'create' or 'overwrite'")
        validate_table_name(name)

        # Get location from namespace
        table_id = namespace + [name]

        # Step 1: Get the table location and storage options from namespace
        # In overwrite mode, if table exists, use describe_table to get
        # existing location. Otherwise, call create_empty_table to reserve
        # a new location
        location = None
        namespace_storage_options = None
        if mode.lower() == "overwrite":
            # Try to describe the table first to see if it exists
            try:
                describe_request = DescribeTableRequest(id=table_id)
                describe_response = self._ns.describe_table(describe_request)
                location = describe_response.location
                namespace_storage_options = describe_response.storage_options
            except Exception:
                # Table doesn't exist, will create a new one below
                pass

        if location is None:
            # Table doesn't exist or mode is "create", reserve a new location
            create_empty_request = CreateEmptyTableRequest(
                id=table_id,
                location=None,
                properties=self.storage_options if self.storage_options else None,
            )
            create_empty_response = self._ns.create_empty_table(create_empty_request)

            if not create_empty_response.location:
                raise ValueError(
                    "Table location is missing from create_empty_table response"
                )

            location = create_empty_response.location
            namespace_storage_options = create_empty_response.storage_options

        # Merge storage options: self.storage_options < user options < namespace options
        merged_storage_options = dict(self.storage_options)
        if storage_options:
            merged_storage_options.update(storage_options)
        if namespace_storage_options:
            merged_storage_options.update(namespace_storage_options)

        # Step 2: Create table using LanceTable.create with the location
        # We need a temporary connection for the LanceTable.create method
        temp_conn = LanceDBConnection(
            location,  # Use the actual location as the connection URI
            read_consistency_interval=self.read_consistency_interval,
            storage_options=merged_storage_options,
            session=self.session,
        )

        # Create a storage options provider if not provided by user
        # Only create if namespace returned storage_options (not None)
        if storage_options_provider is None and namespace_storage_options is not None:
            storage_options_provider = LanceNamespaceStorageOptionsProvider(
                namespace=self._ns,
                table_id=table_id,
            )

        tbl = LanceTable.create(
            temp_conn,
            name,
            data,
            schema,
            mode=mode,
            exist_ok=exist_ok,
            on_bad_vectors=on_bad_vectors,
            fill_value=fill_value,
            embedding_functions=embedding_functions,
            namespace=namespace,
            storage_options=merged_storage_options,
            storage_options_provider=storage_options_provider,
            location=location,
        )

        return tbl

    @override
    def open_table(
        self,
        name: str,
        *,
        namespace: Optional[List[str]] = None,
        storage_options: Optional[Dict[str, str]] = None,
        storage_options_provider: Optional[StorageOptionsProvider] = None,
        index_cache_size: Optional[int] = None,
    ) -> Table:
        if namespace is None:
            namespace = []
        table_id = namespace + [name]
        request = DescribeTableRequest(id=table_id)
        response = self._ns.describe_table(request)

        # Merge storage options: self.storage_options < user options < namespace options
        merged_storage_options = dict(self.storage_options)
        if storage_options:
            merged_storage_options.update(storage_options)
        if response.storage_options:
            merged_storage_options.update(response.storage_options)

        # Create a storage options provider if not provided by user
        # Only create if namespace returned storage_options (not None)
        if storage_options_provider is None and response.storage_options is not None:
            storage_options_provider = LanceNamespaceStorageOptionsProvider(
                namespace=self._ns,
                table_id=table_id,
            )

        return self._lance_table_from_uri(
            name,
            response.location,
            namespace=namespace,
            storage_options=merged_storage_options,
            storage_options_provider=storage_options_provider,
            index_cache_size=index_cache_size,
        )

    @override
    def drop_table(self, name: str, namespace: Optional[List[str]] = None):
        # Use namespace drop_table directly
        if namespace is None:
            namespace = []
        table_id = namespace + [name]
        request = DropTableRequest(id=table_id)
        self._ns.drop_table(request)

    @override
    def rename_table(
        self,
        cur_name: str,
        new_name: str,
        cur_namespace: Optional[List[str]] = None,
        new_namespace: Optional[List[str]] = None,
    ):
        if cur_namespace is None:
            cur_namespace = []
        if new_namespace is None:
            new_namespace = []
        raise NotImplementedError(
            "rename_table is not supported for namespace connections"
        )

    @override
    def drop_database(self):
        raise NotImplementedError(
            "drop_database is deprecated, use drop_all_tables instead"
        )

    @override
    def drop_all_tables(self, namespace: Optional[List[str]] = None):
        if namespace is None:
            namespace = []
        for table_name in self.table_names(namespace=namespace):
            self.drop_table(table_name, namespace=namespace)

    @override
    def list_namespaces(
        self,
        namespace: Optional[List[str]] = None,
        page_token: Optional[str] = None,
        limit: Optional[int] = None,
    ) -> ListNamespacesResponse:
        """
        List child namespaces under the given namespace.

        Parameters
        ----------
        namespace : Optional[List[str]]
            The parent namespace to list children from.
            If None, lists root-level namespaces.
        page_token : Optional[str]
            Token for pagination. Use the token from a previous response
            to get the next page of results.
        limit : int, optional
            Maximum number of namespaces to return.

        Returns
        -------
        ListNamespacesResponse
            Response containing namespace names and optional page_token for pagination.
        """
        if namespace is None:
            namespace = []
        request = ListNamespacesRequest(
            id=namespace, page_token=page_token, limit=limit
        )
        response = self._ns.list_namespaces(request)
        return ListNamespacesResponse(
            namespaces=response.namespaces if response.namespaces else [],
            page_token=response.page_token,
        )

    @override
    def create_namespace(
        self,
        namespace: List[str],
        mode: Optional[str] = None,
        properties: Optional[Dict[str, str]] = None,
    ) -> CreateNamespaceResponse:
        """
        Create a new namespace.

        Parameters
        ----------
        namespace : List[str]
            The namespace path to create.
        mode : str, optional
            Creation mode - "create" (fail if exists), "exist_ok" (skip if exists),
            or "overwrite" (replace if exists). Case insensitive.
        properties : Dict[str, str], optional
            Properties to set on the namespace.

        Returns
        -------
        CreateNamespaceResponse
            Response containing the properties of the created namespace.
        """
        request = CreateNamespaceRequest(
            id=namespace,
            mode=_normalize_create_namespace_mode(mode),
            properties=properties,
        )
        response = self._ns.create_namespace(request)
        return CreateNamespaceResponse(
            properties=response.properties if hasattr(response, "properties") else None
        )

    @override
    def drop_namespace(
        self,
        namespace: List[str],
        mode: Optional[str] = None,
        behavior: Optional[str] = None,
    ) -> DropNamespaceResponse:
        """
        Drop a namespace.

        Parameters
        ----------
        namespace : List[str]
            The namespace path to drop.
        mode : str, optional
            Whether to skip if not exists ("SKIP") or fail ("FAIL"). Case insensitive.
        behavior : str, optional
            Whether to restrict drop if not empty ("RESTRICT") or cascade ("CASCADE").
            Case insensitive.

        Returns
        -------
        DropNamespaceResponse
            Response containing properties and transaction_id if applicable.
        """
        request = DropNamespaceRequest(
            id=namespace,
            mode=_normalize_drop_namespace_mode(mode),
            behavior=_normalize_drop_namespace_behavior(behavior),
        )
        response = self._ns.drop_namespace(request)
        return DropNamespaceResponse(
            properties=(
                response.properties if hasattr(response, "properties") else None
            ),
            transaction_id=(
                response.transaction_id if hasattr(response, "transaction_id") else None
            ),
        )

    @override
    def describe_namespace(self, namespace: List[str]) -> DescribeNamespaceResponse:
        """
        Describe a namespace.

        Parameters
        ----------
        namespace : List[str]
            The namespace identifier to describe.

        Returns
        -------
        DescribeNamespaceResponse
            Response containing the namespace properties.
        """
        request = DescribeNamespaceRequest(id=namespace)
        response = self._ns.describe_namespace(request)
        return DescribeNamespaceResponse(
            properties=response.properties if hasattr(response, "properties") else None
        )

    @override
    def list_tables(
        self,
        namespace: Optional[List[str]] = None,
        page_token: Optional[str] = None,
        limit: Optional[int] = None,
    ) -> ListTablesResponse:
        """
        List all tables in this database with pagination support.

        Parameters
        ----------
        namespace : List[str], optional
            The namespace to list tables in.
            None or empty list represents root namespace.
        page_token : str, optional
            Token for pagination. Use the token from a previous response
            to get the next page of results.
        limit : int, optional
            The maximum number of results to return.

        Returns
        -------
        ListTablesResponse
            Response containing table names and optional page_token for pagination.
        """
        if namespace is None:
            namespace = []
        request = ListTablesRequest(id=namespace, page_token=page_token, limit=limit)
        response = self._ns.list_tables(request)
        return ListTablesResponse(
            tables=response.tables if response.tables else [],
            page_token=response.page_token,
        )

    def _lance_table_from_uri(
        self,
        name: str,
        table_uri: str,
        *,
        namespace: Optional[List[str]] = None,
        storage_options: Optional[Dict[str, str]] = None,
        storage_options_provider: Optional[StorageOptionsProvider] = None,
        index_cache_size: Optional[int] = None,
    ) -> LanceTable:
        # Open a table directly from a URI using the location parameter
        # Note: storage_options should already be merged by the caller
        if namespace is None:
            namespace = []
        temp_conn = LanceDBConnection(
            table_uri,  # Use the table location as the connection URI
            read_consistency_interval=self.read_consistency_interval,
            storage_options=storage_options if storage_options is not None else {},
            session=self.session,
        )

        # Open the table using the temporary connection with the location parameter
        return LanceTable.open(
            temp_conn,
            name,
            namespace=namespace,
            storage_options=storage_options,
            storage_options_provider=storage_options_provider,
            index_cache_size=index_cache_size,
            location=table_uri,
        )


class AsyncLanceNamespaceDBConnection:
    """
    An async LanceDB connection that uses a namespace for table management.

    This connection delegates table URI resolution to a lance_namespace instance,
    while providing async methods for all operations.
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
        Initialize an async namespace-based LanceDB connection.

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

    async def table_names(
        self,
        page_token: Optional[str] = None,
        limit: int = 10,
        *,
        namespace: Optional[List[str]] = None,
    ) -> Iterable[str]:
        """
        List table names in the namespace.

        .. deprecated::
            Use :meth:`list_tables` instead, which provides proper pagination support.
        """
        import warnings

        warnings.warn(
            "table_names() is deprecated, use list_tables() instead",
            DeprecationWarning,
            stacklevel=2,
        )
        if namespace is None:
            namespace = []
        request = ListTablesRequest(id=namespace, page_token=page_token, limit=limit)
        response = self._ns.list_tables(request)
        return response.tables if response.tables else []

    async def create_table(
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
        namespace: Optional[List[str]] = None,
        storage_options: Optional[Dict[str, str]] = None,
        storage_options_provider: Optional[StorageOptionsProvider] = None,
        data_storage_version: Optional[str] = None,
        enable_v2_manifest_paths: Optional[bool] = None,
    ) -> AsyncTable:
        """Create a new table in the namespace."""
        if namespace is None:
            namespace = []
        if mode.lower() not in ["create", "overwrite"]:
            raise ValueError("mode must be either 'create' or 'overwrite'")
        validate_table_name(name)

        # Get location from namespace
        table_id = namespace + [name]

        # Step 1: Get the table location and storage options from namespace
        location = None
        namespace_storage_options = None
        if mode.lower() == "overwrite":
            # Try to describe the table first to see if it exists
            try:
                describe_request = DescribeTableRequest(id=table_id)
                describe_response = self._ns.describe_table(describe_request)
                location = describe_response.location
                namespace_storage_options = describe_response.storage_options
            except Exception:
                # Table doesn't exist, will create a new one below
                pass

        if location is None:
            # Table doesn't exist or mode is "create", reserve a new location
            create_empty_request = CreateEmptyTableRequest(
                id=table_id,
                location=None,
                properties=self.storage_options if self.storage_options else None,
            )
            create_empty_response = self._ns.create_empty_table(create_empty_request)

            if not create_empty_response.location:
                raise ValueError(
                    "Table location is missing from create_empty_table response"
                )

            location = create_empty_response.location
            namespace_storage_options = create_empty_response.storage_options

        # Merge storage options: self.storage_options < user options < namespace options
        merged_storage_options = dict(self.storage_options)
        if storage_options:
            merged_storage_options.update(storage_options)
        if namespace_storage_options:
            merged_storage_options.update(namespace_storage_options)

        # Step 2: Create table using LanceTable.create with the location
        # Run the sync operation in a thread
        def _create_table():
            temp_conn = LanceDBConnection(
                location,
                read_consistency_interval=self.read_consistency_interval,
                storage_options=merged_storage_options,
                session=self.session,
            )

            # Create a storage options provider if not provided by user
            if (
                storage_options_provider is None
                and namespace_storage_options is not None
            ):
                provider = LanceNamespaceStorageOptionsProvider(
                    namespace=self._ns,
                    table_id=table_id,
                )
            else:
                provider = storage_options_provider

            return LanceTable.create(
                temp_conn,
                name,
                data,
                schema,
                mode=mode,
                exist_ok=exist_ok,
                on_bad_vectors=on_bad_vectors,
                fill_value=fill_value,
                embedding_functions=embedding_functions,
                namespace=namespace,
                storage_options=merged_storage_options,
                storage_options_provider=provider,
                location=location,
            )

        lance_table = await asyncio.to_thread(_create_table)
        # Get the underlying async table from LanceTable
        return lance_table._table

    async def open_table(
        self,
        name: str,
        *,
        namespace: Optional[List[str]] = None,
        storage_options: Optional[Dict[str, str]] = None,
        storage_options_provider: Optional[StorageOptionsProvider] = None,
        index_cache_size: Optional[int] = None,
    ) -> AsyncTable:
        """Open an existing table from the namespace."""
        if namespace is None:
            namespace = []
        table_id = namespace + [name]
        request = DescribeTableRequest(id=table_id)
        response = self._ns.describe_table(request)

        # Merge storage options: self.storage_options < user options < namespace options
        merged_storage_options = dict(self.storage_options)
        if storage_options:
            merged_storage_options.update(storage_options)
        if response.storage_options:
            merged_storage_options.update(response.storage_options)

        # Create a storage options provider if not provided by user
        if storage_options_provider is None and response.storage_options is not None:
            storage_options_provider = LanceNamespaceStorageOptionsProvider(
                namespace=self._ns,
                table_id=table_id,
            )

        # Open table in a thread
        def _open_table():
            temp_conn = LanceDBConnection(
                response.location,
                read_consistency_interval=self.read_consistency_interval,
                storage_options=merged_storage_options,
                session=self.session,
            )

            return LanceTable.open(
                temp_conn,
                name,
                namespace=namespace,
                storage_options=merged_storage_options,
                storage_options_provider=storage_options_provider,
                index_cache_size=index_cache_size,
                location=response.location,
            )

        lance_table = await asyncio.to_thread(_open_table)
        return lance_table._table

    async def drop_table(self, name: str, namespace: Optional[List[str]] = None):
        """Drop a table from the namespace."""
        if namespace is None:
            namespace = []
        table_id = namespace + [name]
        request = DropTableRequest(id=table_id)
        self._ns.drop_table(request)

    async def rename_table(
        self,
        cur_name: str,
        new_name: str,
        cur_namespace: Optional[List[str]] = None,
        new_namespace: Optional[List[str]] = None,
    ):
        """Rename is not supported for namespace connections."""
        if cur_namespace is None:
            cur_namespace = []
        if new_namespace is None:
            new_namespace = []
        raise NotImplementedError(
            "rename_table is not supported for namespace connections"
        )

    async def drop_database(self):
        """Deprecated method."""
        raise NotImplementedError(
            "drop_database is deprecated, use drop_all_tables instead"
        )

    async def drop_all_tables(self, namespace: Optional[List[str]] = None):
        """Drop all tables in the namespace."""
        if namespace is None:
            namespace = []
        table_names = await self.table_names(namespace=namespace)
        for table_name in table_names:
            await self.drop_table(table_name, namespace=namespace)

    async def list_namespaces(
        self,
        namespace: Optional[List[str]] = None,
        page_token: Optional[str] = None,
        limit: Optional[int] = None,
    ) -> ListNamespacesResponse:
        """
        List child namespaces under the given namespace.

        Parameters
        ----------
        namespace : Optional[List[str]]
            The parent namespace to list children from.
            If None, lists root-level namespaces.
        page_token : Optional[str]
            Token for pagination. Use the token from a previous response
            to get the next page of results.
        limit : int, optional
            Maximum number of namespaces to return.

        Returns
        -------
        ListNamespacesResponse
            Response containing namespace names and optional page_token for pagination.
        """
        if namespace is None:
            namespace = []
        request = ListNamespacesRequest(
            id=namespace, page_token=page_token, limit=limit
        )
        response = self._ns.list_namespaces(request)
        return ListNamespacesResponse(
            namespaces=response.namespaces if response.namespaces else [],
            page_token=response.page_token,
        )

    async def create_namespace(
        self,
        namespace: List[str],
        mode: Optional[str] = None,
        properties: Optional[Dict[str, str]] = None,
    ) -> CreateNamespaceResponse:
        """
        Create a new namespace.

        Parameters
        ----------
        namespace : List[str]
            The namespace path to create.
        mode : str, optional
            Creation mode - "create" (fail if exists), "exist_ok" (skip if exists),
            or "overwrite" (replace if exists). Case insensitive.
        properties : Dict[str, str], optional
            Properties to set on the namespace.

        Returns
        -------
        CreateNamespaceResponse
            Response containing the properties of the created namespace.
        """
        request = CreateNamespaceRequest(
            id=namespace,
            mode=_normalize_create_namespace_mode(mode),
            properties=properties,
        )
        response = self._ns.create_namespace(request)
        return CreateNamespaceResponse(
            properties=response.properties if hasattr(response, "properties") else None
        )

    async def drop_namespace(
        self,
        namespace: List[str],
        mode: Optional[str] = None,
        behavior: Optional[str] = None,
    ) -> DropNamespaceResponse:
        """
        Drop a namespace.

        Parameters
        ----------
        namespace : List[str]
            The namespace path to drop.
        mode : str, optional
            Whether to skip if not exists ("SKIP") or fail ("FAIL"). Case insensitive.
        behavior : str, optional
            Whether to restrict drop if not empty ("RESTRICT") or cascade ("CASCADE").
            Case insensitive.

        Returns
        -------
        DropNamespaceResponse
            Response containing properties and transaction_id if applicable.
        """
        request = DropNamespaceRequest(
            id=namespace,
            mode=_normalize_drop_namespace_mode(mode),
            behavior=_normalize_drop_namespace_behavior(behavior),
        )
        response = self._ns.drop_namespace(request)
        return DropNamespaceResponse(
            properties=(
                response.properties if hasattr(response, "properties") else None
            ),
            transaction_id=(
                response.transaction_id if hasattr(response, "transaction_id") else None
            ),
        )

    async def describe_namespace(
        self, namespace: List[str]
    ) -> DescribeNamespaceResponse:
        """
        Describe a namespace.

        Parameters
        ----------
        namespace : List[str]
            The namespace identifier to describe.

        Returns
        -------
        DescribeNamespaceResponse
            Response containing the namespace properties.
        """
        request = DescribeNamespaceRequest(id=namespace)
        response = self._ns.describe_namespace(request)
        return DescribeNamespaceResponse(
            properties=response.properties if hasattr(response, "properties") else None
        )

    async def list_tables(
        self,
        namespace: Optional[List[str]] = None,
        page_token: Optional[str] = None,
        limit: Optional[int] = None,
    ) -> ListTablesResponse:
        """
        List all tables in this database with pagination support.

        Parameters
        ----------
        namespace : List[str], optional
            The namespace to list tables in.
            None or empty list represents root namespace.
        page_token : str, optional
            Token for pagination. Use the token from a previous response
            to get the next page of results.
        limit : int, optional
            The maximum number of results to return.

        Returns
        -------
        ListTablesResponse
            Response containing table names and optional page_token for pagination.
        """
        if namespace is None:
            namespace = []
        request = ListTablesRequest(id=namespace, page_token=page_token, limit=limit)
        response = self._ns.list_tables(request)
        return ListTablesResponse(
            tables=response.tables if response.tables else [],
            page_token=response.page_token,
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
    """
    namespace = namespace_connect(impl, properties)

    # Return the namespace-based connection
    return LanceNamespaceDBConnection(
        namespace,
        read_consistency_interval=read_consistency_interval,
        storage_options=storage_options,
        session=session,
    )


def connect_namespace_async(
    impl: str,
    properties: Dict[str, str],
    *,
    read_consistency_interval: Optional[timedelta] = None,
    storage_options: Optional[Dict[str, str]] = None,
    session: Optional[Session] = None,
) -> AsyncLanceNamespaceDBConnection:
    """
    Connect to a LanceDB database through a namespace (returns async connection).

    This function is synchronous but returns an AsyncLanceNamespaceDBConnection
    that provides async methods for all database operations.

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
    AsyncLanceNamespaceDBConnection
        An async namespace-based connection to LanceDB

    Examples
    --------
    >>> import lancedb
    >>> # This function is sync, but returns an async connection
    >>> db = lancedb.connect_namespace_async("dir", {"root": "/path/to/db"})
    >>> # Use async methods on the connection
    >>> async def use_db():
    ...     tables = await db.table_names()
    ...     table = await db.create_table("my_table", schema=schema)
    """
    namespace = namespace_connect(impl, properties)

    # Return the async namespace-based connection
    return AsyncLanceNamespaceDBConnection(
        namespace,
        read_consistency_interval=read_consistency_interval,
        storage_options=storage_options,
        session=session,
    )
