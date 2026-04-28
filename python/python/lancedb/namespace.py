# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

"""
LanceDB Namespace integration module.

This module provides integration with lance_namespace for managing tables
through a namespace abstraction.
"""

from __future__ import annotations

import sys
from typing import TYPE_CHECKING, Any, Dict, Iterable, List, Optional, Union

if sys.version_info >= (3, 12):
    from typing import override
else:
    from overrides import override

if TYPE_CHECKING:
    from lancedb.query import Query

from datetime import timedelta
import pyarrow as pa

from lance_namespace_urllib3_client.models.json_arrow_data_type import JsonArrowDataType
from lance_namespace_urllib3_client.models.json_arrow_field import JsonArrowField
from lance_namespace_urllib3_client.models.json_arrow_schema import JsonArrowSchema
from lance_namespace_urllib3_client.models.query_table_request import QueryTableRequest
from lance_namespace_urllib3_client.models.query_table_request_columns import (
    QueryTableRequestColumns,
)
from lance_namespace_urllib3_client.models.query_table_request_full_text_query import (
    QueryTableRequestFullTextQuery,
)
from lance_namespace_urllib3_client.models.query_table_request_vector import (
    QueryTableRequestVector,
)
from lance_namespace_urllib3_client.models.string_fts_query import StringFtsQuery
from lance_namespace.errors import TableNotFoundError
from lancedb._lancedb import connect_namespace_client as _connect_namespace_client
from lancedb.background_loop import LOOP
from lancedb.db import AsyncConnection, DBConnection
from lancedb.namespace_utils import (
    _normalize_create_namespace_mode,
    _normalize_drop_namespace_mode,
    _normalize_drop_namespace_behavior,
)
from lance_namespace import (
    LanceNamespace,
    connect as namespace_connect,
    CreateNamespaceResponse,
    DescribeNamespaceResponse,
    DropNamespaceResponse,
    ListNamespacesResponse,
    ListTablesResponse,
    ListTablesRequest,
    DescribeNamespaceRequest,
    DropTableRequest,
    ListNamespacesRequest,
    CreateNamespaceRequest,
    DropNamespaceRequest,
)
from lancedb.table import AsyncTable, LanceTable, Table
from lancedb.util import validate_table_name
from lancedb.common import DATA
from lancedb.pydantic import LanceModel
from lancedb.embeddings import EmbeddingFunctionConfig
from ._lancedb import Session


def _query_to_namespace_request(
    table_id: List[str],
    query: "Query",
) -> QueryTableRequest:
    """
    Convert a LanceDB Query object to a QueryTableRequest for server-side query.

    Parameters
    ----------
    table_id : List[str]
        The table identifier (namespace path + table name)
    query : Query
        The LanceDB Query object to convert

    Returns
    -------
    QueryTableRequest
        The namespace QueryTableRequest
    """
    from lancedb.query import Query

    if not isinstance(query, Query):
        raise TypeError(f"Expected Query, got {type(query)}")

    vector_request = None
    if query.vector is not None:
        if isinstance(query.vector, pa.Array):
            vector_list = query.vector.to_pylist()
        elif (
            isinstance(query.vector, list)
            and query.vector
            and isinstance(query.vector[0], pa.Array)
        ):
            vector_list = [v.to_pylist() for v in query.vector]
        else:
            vector_list = query.vector

        if (
            isinstance(vector_list, list)
            and vector_list
            and isinstance(vector_list[0], list)
        ):
            vector_request = QueryTableRequestVector(multi_vector=vector_list)
        else:
            vector_request = QueryTableRequestVector(single_vector=vector_list)

    columns_request = None
    if query.columns is not None:
        if isinstance(query.columns, dict):
            columns_request = QueryTableRequestColumns(column_aliases=query.columns)
        else:
            columns_request = QueryTableRequestColumns(column_names=query.columns)

    full_text_query_request = None
    if query.full_text_query is not None:
        fts_query = query.full_text_query
        if isinstance(fts_query, str):
            full_text_query_request = QueryTableRequestFullTextQuery(
                string_query=StringFtsQuery(query=fts_query)
            )
        elif hasattr(fts_query, "query"):
            full_text_query_request = QueryTableRequestFullTextQuery(
                string_query=StringFtsQuery(
                    query=fts_query.query,
                    columns=fts_query.columns
                    if hasattr(fts_query, "columns")
                    else None,
                )
            )

    prefilter = None
    if query.postfilter is not None:
        prefilter = not query.postfilter

    k = query.limit if query.limit is not None else 10

    # Build request kwargs, only including non-None values for optional fields
    # that Pydantic doesn't accept as None
    kwargs: dict = {
        "id": table_id,
        "k": k,
        # vector is required in the model but can be empty for non-vector queries
        "vector": vector_request
        if vector_request is not None
        else QueryTableRequestVector(),
    }
    if query.vector_column is not None:
        kwargs["vector_column"] = query.vector_column
    if query.filter is not None:
        kwargs["filter"] = query.filter
    if prefilter is not None:
        kwargs["prefilter"] = prefilter
    if full_text_query_request is not None:
        kwargs["full_text_query"] = full_text_query_request
    if columns_request is not None:
        kwargs["columns"] = columns_request
    if query.offset is not None:
        kwargs["offset"] = query.offset
    if query.with_row_id is not None:
        kwargs["with_row_id"] = query.with_row_id
    if query.ef is not None:
        kwargs["ef"] = query.ef
    if query.minimum_nprobes is not None:
        kwargs["nprobes"] = query.minimum_nprobes
    if query.refine_factor is not None:
        kwargs["refine_factor"] = query.refine_factor
    if query.lower_bound is not None:
        kwargs["lower_bound"] = query.lower_bound
    if query.upper_bound is not None:
        kwargs["upper_bound"] = query.upper_bound
    if query.bypass_vector_index is not None:
        kwargs["bypass_vector_index"] = query.bypass_vector_index
    if query.fast_search is not None:
        kwargs["fast_search"] = query.fast_search
    if query.distance_type is not None:
        kwargs["distance_type"] = query.distance_type

    return QueryTableRequest(**kwargs)


def _execute_server_side_query(
    namespace_client: LanceNamespace,
    table_id: List[str],
    query: "Query",
) -> pa.RecordBatchReader:
    """
    Execute a query on the namespace server and return results as RecordBatchReader.

    Parameters
    ----------
    namespace_client : LanceNamespace
        The namespace client to use
    table_id : List[str]
        The table identifier (namespace path + table name)
    query : Query
        The LanceDB Query object

    Returns
    -------
    pa.RecordBatchReader
        The query results as a RecordBatchReader
    """
    request = _query_to_namespace_request(table_id, query)
    ipc_bytes = namespace_client.query_table(request)
    return _arrow_ipc_to_record_batch_reader(ipc_bytes)


def _data_to_arrow_ipc(
    data: Optional["DATA"],
    schema: Optional[Union[pa.Schema, LanceModel]],
    embedding_functions: Optional[List[EmbeddingFunctionConfig]] = None,
    on_bad_vectors: str = "error",
    fill_value: float = 0.0,
) -> bytes:
    """
    Convert data to Arrow IPC format (file format) for server-side create_table.

    If data is None but schema is provided, creates an empty IPC stream with
    just the schema in the header (zero batches). This is used to create
    empty tables with just a schema.

    Returns the serialized Arrow IPC bytes.
    """
    from lancedb.embeddings import EmbeddingFunctionRegistry
    from lancedb.table import _sanitize_data

    if isinstance(schema, type) and issubclass(schema, LanceModel):
        schema = schema.to_arrow_schema()

    # Get embedding metadata (same pattern as db.py create_table)
    registry = EmbeddingFunctionRegistry.get_instance()
    if embedding_functions:
        metadata = registry.get_table_metadata(embedding_functions)
    elif schema is not None:
        metadata = schema.metadata
    else:
        metadata = None

    # Handle schema-only table creation (no data)
    if data is None:
        if schema is None:
            raise ValueError(
                "Either data or schema is required for server-side create_table"
            )
        # Create empty IPC stream with just the schema in the header
        if metadata:
            schema = schema.with_metadata(metadata)
        sink = pa.BufferOutputStream()
        with pa.ipc.new_stream(sink, schema) as writer:
            pass  # No batches to write, just schema in header
        return sink.getvalue().to_pybytes()

    reader = _sanitize_data(
        data,
        target_schema=schema,
        metadata=metadata,
        on_bad_vectors=on_bad_vectors,
        fill_value=fill_value,
    )

    sink = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink, reader.schema) as writer:
        for batch in reader:
            writer.write_batch(batch)

    return sink.getvalue().to_pybytes()


def _arrow_ipc_to_record_batch_reader(ipc_bytes: bytes) -> pa.RecordBatchReader:
    """
    Convert Arrow IPC bytes to a RecordBatchReader.
    """
    buf = pa.py_buffer(ipc_bytes)
    reader = pa.ipc.open_file(buf)
    batches = [reader.get_batch(i) for i in range(reader.num_record_batches)]
    return pa.RecordBatchReader.from_batches(reader.schema, batches)


def _normalize_create_table_mode(mode: str) -> str:
    """Normalize create table mode string for namespace API."""
    mode_lower = mode.lower()
    if mode_lower == "create":
        return "Create"
    elif mode_lower == "overwrite":
        return "Overwrite"
    else:
        raise ValueError(f"Invalid mode: {mode}. Must be 'create' or 'overwrite'")


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


class LanceNamespaceDBConnection(DBConnection):
    """
    A LanceDB connection that uses a namespace for table management.

    This connection delegates table URI resolution to a lance_namespace instance,
    while using the standard LanceTable for actual table operations.
    """

    def __init__(
        self,
        namespace_client: LanceNamespace,
        *,
        read_consistency_interval: Optional[timedelta] = None,
        storage_options: Optional[Dict[str, str]] = None,
        session: Optional[Session] = None,
        namespace_client_pushdown_operations: Optional[List[str]] = None,
        namespace_client_impl: Optional[str] = None,
        namespace_client_properties: Optional[Dict[str, str]] = None,
    ):
        """
        Initialize a namespace-based LanceDB connection.

        Parameters
        ----------
        namespace_client : LanceNamespace
            The namespace client to use for table management
        read_consistency_interval : Optional[timedelta]
            The interval at which to check for updates to the table from other
            processes. If None, then consistency is not checked.
        storage_options : Optional[Dict[str, str]]
            Additional options for the storage backend
        session : Optional[Session]
            A session to use for this connection
        namespace_client_pushdown_operations : Optional[List[str]]
            List of namespace operations to push down to the namespace server.
            Supported values:

            - "QueryTable": Execute queries on the namespace server via
              namespace.query_table() instead of locally.
            - "CreateTable": Execute table creation on the namespace server via
              namespace.create_table() instead of using declare_table + local write.

            Default is None (no pushdown, all operations run locally).
        namespace_client_impl : Optional[str]
            The namespace implementation name used to create this connection.
            Stored for serialization purposes.
        namespace_client_properties : Optional[Dict[str, str]]
            The namespace properties used to create this connection.
            Stored for serialization purposes.
        """
        self._namespace_client = namespace_client
        self.read_consistency_interval = read_consistency_interval
        self.storage_options = storage_options or {}
        self.session = session
        self._namespace_client_pushdown_operations = set(
            namespace_client_pushdown_operations or []
        )
        self._namespace_client_impl = namespace_client_impl
        self._namespace_client_properties = namespace_client_properties
        self._inner = AsyncConnection(
            _connect_namespace_client(
                namespace_client,
                read_consistency_interval=(
                    read_consistency_interval.total_seconds()
                    if read_consistency_interval is not None
                    else None
                ),
                storage_options=self.storage_options or None,
                session=session,
                namespace_client_pushdown_operations=(
                    list(self._namespace_client_pushdown_operations)
                ),
                namespace_client_impl=namespace_client_impl,
                namespace_client_properties=namespace_client_properties,
            )
        )

    @override
    def serialize(self) -> str:
        import json

        return json.dumps(
            {
                "connection_type": "namespace",
                "namespace_client_impl": self._namespace_client_impl,
                "namespace_client_properties": self._namespace_client_properties,
                "namespace_client_pushdown_operations": sorted(
                    self._namespace_client_pushdown_operations
                ),
                "storage_options": self.storage_options or None,
                "read_consistency_interval_seconds": (
                    self.read_consistency_interval.total_seconds()
                    if self.read_consistency_interval
                    else None
                ),
            }
        )

    @override
    def table_names(
        self,
        page_token: Optional[str] = None,
        limit: int = 10,
        *,
        namespace_path: Optional[List[str]] = None,
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
        if namespace_path is None:
            namespace_path = []
        request = ListTablesRequest(
            id=namespace_path, page_token=page_token, limit=limit
        )
        response = self._namespace_client.list_tables(request)
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
        namespace_path: Optional[List[str]] = None,
        storage_options: Optional[Dict[str, str]] = None,
        data_storage_version: Optional[str] = None,
        enable_v2_manifest_paths: Optional[bool] = None,
    ) -> Table:
        if namespace_path is None:
            namespace_path = []
        if mode.lower() not in ["create", "overwrite"]:
            raise ValueError("mode must be either 'create' or 'overwrite'")
        validate_table_name(name)
        async_table = LOOP.run(
            self._inner.create_table(
                name,
                data,
                schema=schema,
                mode=mode,
                exist_ok=exist_ok,
                on_bad_vectors=on_bad_vectors,
                fill_value=fill_value,
                embedding_functions=embedding_functions,
                namespace_path=namespace_path,
                storage_options=storage_options,
            )
        )

        return LanceTable(
            self,
            name,
            namespace_path=namespace_path,
            namespace_client=self._namespace_client,
            pushdown_operations=self._namespace_client_pushdown_operations,
            _async=async_table,
        )

    @override
    def open_table(
        self,
        name: str,
        *,
        namespace_path: Optional[List[str]] = None,
        storage_options: Optional[Dict[str, str]] = None,
        index_cache_size: Optional[int] = None,
    ) -> Table:
        if namespace_path is None:
            namespace_path = []
        try:
            async_table = LOOP.run(
                self._inner.open_table(
                    name,
                    namespace_path=namespace_path,
                    storage_options=storage_options,
                    index_cache_size=index_cache_size,
                )
            )
        except RuntimeError as e:
            if "Table not found" in str(e):
                table_id = namespace_path + [name]
                raise TableNotFoundError(f"Table not found: {'$'.join(table_id)}")
            raise

        return LanceTable(
            self,
            name,
            namespace_path=namespace_path,
            namespace_client=self._namespace_client,
            pushdown_operations=self._namespace_client_pushdown_operations,
            _async=async_table,
        )

    @override
    def drop_table(self, name: str, namespace_path: Optional[List[str]] = None):
        # Use namespace drop_table directly
        if namespace_path is None:
            namespace_path = []
        table_id = namespace_path + [name]
        request = DropTableRequest(id=table_id)
        self._namespace_client.drop_table(request)

    @override
    def rename_table(
        self,
        cur_name: str,
        new_name: str,
        cur_namespace_path: Optional[List[str]] = None,
        new_namespace_path: Optional[List[str]] = None,
    ):
        if cur_namespace_path is None:
            cur_namespace_path = []
        if new_namespace_path is None:
            new_namespace_path = []
        raise NotImplementedError(
            "rename_table is not supported for namespace connections"
        )

    @override
    def drop_database(self):
        raise NotImplementedError(
            "drop_database is deprecated, use drop_all_tables instead"
        )

    @override
    def drop_all_tables(self, namespace_path: Optional[List[str]] = None):
        if namespace_path is None:
            namespace_path = []
        for table_name in self.table_names(namespace_path=namespace_path):
            self.drop_table(table_name, namespace_path=namespace_path)

    @override
    def list_namespaces(
        self,
        namespace_path: Optional[List[str]] = None,
        page_token: Optional[str] = None,
        limit: Optional[int] = None,
    ) -> ListNamespacesResponse:
        """
        List child namespaces under the given namespace.

        Parameters
        ----------
        namespace_path : Optional[List[str]]
            The parent namespace path to list children from.
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
        if namespace_path is None:
            namespace_path = []
        request = ListNamespacesRequest(
            id=namespace_path, page_token=page_token, limit=limit
        )
        response = self._namespace_client.list_namespaces(request)
        return ListNamespacesResponse(
            namespaces=response.namespaces if response.namespaces else [],
            page_token=response.page_token,
        )

    @override
    def create_namespace(
        self,
        namespace_path: List[str],
        mode: Optional[str] = None,
        properties: Optional[Dict[str, str]] = None,
    ) -> CreateNamespaceResponse:
        """
        Create a new namespace.

        Parameters
        ----------
        namespace_path : List[str]
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
            id=namespace_path,
            mode=_normalize_create_namespace_mode(mode),
            properties=properties,
        )
        response = self._namespace_client.create_namespace(request)
        return CreateNamespaceResponse(
            properties=response.properties if hasattr(response, "properties") else None
        )

    @override
    def drop_namespace(
        self,
        namespace_path: List[str],
        mode: Optional[str] = None,
        behavior: Optional[str] = None,
    ) -> DropNamespaceResponse:
        """
        Drop a namespace.

        Parameters
        ----------
        namespace_path : List[str]
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
            id=namespace_path,
            mode=_normalize_drop_namespace_mode(mode),
            behavior=_normalize_drop_namespace_behavior(behavior),
        )
        response = self._namespace_client.drop_namespace(request)
        return DropNamespaceResponse(
            properties=(
                response.properties if hasattr(response, "properties") else None
            ),
            transaction_id=(
                response.transaction_id if hasattr(response, "transaction_id") else None
            ),
        )

    @override
    def describe_namespace(
        self, namespace_path: List[str]
    ) -> DescribeNamespaceResponse:
        """
        Describe a namespace.

        Parameters
        ----------
        namespace_path : List[str]
            The namespace identifier to describe.

        Returns
        -------
        DescribeNamespaceResponse
            Response containing the namespace properties.
        """
        request = DescribeNamespaceRequest(id=namespace_path)
        response = self._namespace_client.describe_namespace(request)
        return DescribeNamespaceResponse(
            properties=response.properties if hasattr(response, "properties") else None
        )

    @override
    def list_tables(
        self,
        namespace_path: Optional[List[str]] = None,
        page_token: Optional[str] = None,
        limit: Optional[int] = None,
    ) -> ListTablesResponse:
        """
        List all tables in this database with pagination support.

        Parameters
        ----------
        namespace_path : List[str], optional
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
        if namespace_path is None:
            namespace_path = []
        request = ListTablesRequest(
            id=namespace_path, page_token=page_token, limit=limit
        )
        response = self._namespace_client.list_tables(request)
        return ListTablesResponse(
            tables=response.tables if response.tables else [],
            page_token=response.page_token,
        )

    def _lance_table_from_uri(
        self,
        name: str,
        table_uri: str,
        *,
        namespace_path: Optional[List[str]] = None,
        storage_options: Optional[Dict[str, str]] = None,
        index_cache_size: Optional[int] = None,
        namespace_client: Optional[Any] = None,
        managed_versioning: Optional[bool] = None,
    ) -> LanceTable:
        # Open a table directly from the namespace-resolved physical location.
        #
        # Open the table through the Rust namespace-backed connection.  The Rust
        # layer keeps the logical namespace path and namespace client intact.
        if namespace_path is None:
            namespace_path = []

        async_table = LOOP.run(
            self._inner.open_table(
                name,
                namespace_path=namespace_path,
                storage_options=storage_options,
                index_cache_size=index_cache_size,
                location=None,
                namespace_client=namespace_client,
                managed_versioning=managed_versioning,
            )
        )

        return LanceTable(
            self,
            name,
            namespace_path=namespace_path,
            location=table_uri,
            namespace_client=namespace_client,
            managed_versioning=managed_versioning,
            pushdown_operations=self._namespace_client_pushdown_operations,
            _async=async_table,
        )

    @override
    def namespace_client(self) -> LanceNamespace:
        """Get the namespace client for this connection.

        For namespace connections, this returns the backing namespace client
        that was provided during construction.

        Returns
        -------
        LanceNamespace
            The namespace client for this connection.
        """
        return self._namespace_client


class AsyncLanceNamespaceDBConnection:
    """
    An async LanceDB connection that uses a namespace for table management.

    This connection delegates table URI resolution to a lance_namespace instance,
    while providing async methods for all operations.
    """

    def __init__(
        self,
        namespace_client: LanceNamespace,
        *,
        read_consistency_interval: Optional[timedelta] = None,
        storage_options: Optional[Dict[str, str]] = None,
        session: Optional[Session] = None,
        namespace_client_pushdown_operations: Optional[List[str]] = None,
    ):
        """
        Initialize an async namespace-based LanceDB connection.

        Parameters
        ----------
        namespace_client : LanceNamespace
            The namespace client to use for table management
        read_consistency_interval : Optional[timedelta]
            The interval at which to check for updates to the table from other
            processes. If None, then consistency is not checked.
        storage_options : Optional[Dict[str, str]]
            Additional options for the storage backend
        session : Optional[Session]
            A session to use for this connection
        namespace_client_pushdown_operations : Optional[List[str]]
            List of namespace operations to push down to the namespace server.
            Supported values:

            - "QueryTable": Execute queries on the namespace server via
              namespace.query_table() instead of locally.
            - "CreateTable": Execute table creation on the namespace server via
              namespace.create_table() instead of using declare_table + local write.

            Default is None (no pushdown, all operations run locally).
        """
        self._namespace_client = namespace_client
        self.read_consistency_interval = read_consistency_interval
        self.storage_options = storage_options or {}
        self.session = session
        self._namespace_client_pushdown_operations = set(
            namespace_client_pushdown_operations or []
        )
        self._inner = AsyncConnection(
            _connect_namespace_client(
                namespace_client,
                read_consistency_interval=(
                    read_consistency_interval.total_seconds()
                    if read_consistency_interval is not None
                    else None
                ),
                storage_options=self.storage_options or None,
                session=session,
                namespace_client_pushdown_operations=(
                    list(self._namespace_client_pushdown_operations)
                ),
                namespace_client_impl=None,
                namespace_client_properties=None,
            )
        )

    async def table_names(
        self,
        page_token: Optional[str] = None,
        limit: int = 10,
        *,
        namespace_path: Optional[List[str]] = None,
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
        if namespace_path is None:
            namespace_path = []
        request = ListTablesRequest(
            id=namespace_path, page_token=page_token, limit=limit
        )
        response = self._namespace_client.list_tables(request)
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
        namespace_path: Optional[List[str]] = None,
        storage_options: Optional[Dict[str, str]] = None,
        data_storage_version: Optional[str] = None,
        enable_v2_manifest_paths: Optional[bool] = None,
    ) -> AsyncTable:
        """Create a new table in the namespace."""
        if namespace_path is None:
            namespace_path = []
        if mode.lower() not in ["create", "overwrite"]:
            raise ValueError("mode must be either 'create' or 'overwrite'")
        validate_table_name(name)
        return await self._inner.create_table(
            name,
            data,
            schema=schema,
            mode=mode,
            exist_ok=exist_ok,
            on_bad_vectors=on_bad_vectors,
            fill_value=fill_value,
            namespace_path=namespace_path,
            embedding_functions=embedding_functions,
            storage_options=storage_options,
        )

    async def open_table(
        self,
        name: str,
        *,
        namespace_path: Optional[List[str]] = None,
        storage_options: Optional[Dict[str, str]] = None,
        index_cache_size: Optional[int] = None,
    ) -> AsyncTable:
        """Open an existing table from the namespace."""
        if namespace_path is None:
            namespace_path = []
        try:
            return await self._inner.open_table(
                name,
                namespace_path=namespace_path,
                storage_options=storage_options,
                index_cache_size=index_cache_size,
            )
        except RuntimeError as e:
            if "Table not found" in str(e):
                table_id = namespace_path + [name]
                raise TableNotFoundError(f"Table not found: {'$'.join(table_id)}")
            raise

    async def drop_table(self, name: str, namespace_path: Optional[List[str]] = None):
        """Drop a table from the namespace."""
        if namespace_path is None:
            namespace_path = []
        table_id = namespace_path + [name]
        request = DropTableRequest(id=table_id)
        self._namespace_client.drop_table(request)

    async def rename_table(
        self,
        cur_name: str,
        new_name: str,
        cur_namespace_path: Optional[List[str]] = None,
        new_namespace_path: Optional[List[str]] = None,
    ):
        """Rename is not supported for namespace connections."""
        if cur_namespace_path is None:
            cur_namespace_path = []
        if new_namespace_path is None:
            new_namespace_path = []
        raise NotImplementedError(
            "rename_table is not supported for namespace connections"
        )

    async def drop_database(self):
        """Deprecated method."""
        raise NotImplementedError(
            "drop_database is deprecated, use drop_all_tables instead"
        )

    async def drop_all_tables(self, namespace_path: Optional[List[str]] = None):
        """Drop all tables in the namespace."""
        if namespace_path is None:
            namespace_path = []
        table_names = await self.table_names(namespace_path=namespace_path)
        for table_name in table_names:
            await self.drop_table(table_name, namespace_path=namespace_path)

    async def list_namespaces(
        self,
        namespace_path: Optional[List[str]] = None,
        page_token: Optional[str] = None,
        limit: Optional[int] = None,
    ) -> ListNamespacesResponse:
        """
        List child namespaces under the given namespace.

        Parameters
        ----------
        namespace_path : Optional[List[str]]
            The parent namespace path to list children from.
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
        if namespace_path is None:
            namespace_path = []
        request = ListNamespacesRequest(
            id=namespace_path, page_token=page_token, limit=limit
        )
        response = self._namespace_client.list_namespaces(request)
        return ListNamespacesResponse(
            namespaces=response.namespaces if response.namespaces else [],
            page_token=response.page_token,
        )

    async def create_namespace(
        self,
        namespace_path: List[str],
        mode: Optional[str] = None,
        properties: Optional[Dict[str, str]] = None,
    ) -> CreateNamespaceResponse:
        """
        Create a new namespace.

        Parameters
        ----------
        namespace_path : List[str]
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
            id=namespace_path,
            mode=_normalize_create_namespace_mode(mode),
            properties=properties,
        )
        response = self._namespace_client.create_namespace(request)
        return CreateNamespaceResponse(
            properties=response.properties if hasattr(response, "properties") else None
        )

    async def drop_namespace(
        self,
        namespace_path: List[str],
        mode: Optional[str] = None,
        behavior: Optional[str] = None,
    ) -> DropNamespaceResponse:
        """
        Drop a namespace.

        Parameters
        ----------
        namespace_path : List[str]
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
            id=namespace_path,
            mode=_normalize_drop_namespace_mode(mode),
            behavior=_normalize_drop_namespace_behavior(behavior),
        )
        response = self._namespace_client.drop_namespace(request)
        return DropNamespaceResponse(
            properties=(
                response.properties if hasattr(response, "properties") else None
            ),
            transaction_id=(
                response.transaction_id if hasattr(response, "transaction_id") else None
            ),
        )

    async def describe_namespace(
        self, namespace_path: List[str]
    ) -> DescribeNamespaceResponse:
        """
        Describe a namespace.

        Parameters
        ----------
        namespace_path : List[str]
            The namespace identifier to describe.

        Returns
        -------
        DescribeNamespaceResponse
            Response containing the namespace properties.
        """
        request = DescribeNamespaceRequest(id=namespace_path)
        response = self._namespace_client.describe_namespace(request)
        return DescribeNamespaceResponse(
            properties=response.properties if hasattr(response, "properties") else None
        )

    async def list_tables(
        self,
        namespace_path: Optional[List[str]] = None,
        page_token: Optional[str] = None,
        limit: Optional[int] = None,
    ) -> ListTablesResponse:
        """
        List all tables in this database with pagination support.

        Parameters
        ----------
        namespace_path : List[str], optional
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
        if namespace_path is None:
            namespace_path = []
        request = ListTablesRequest(
            id=namespace_path, page_token=page_token, limit=limit
        )
        response = self._namespace_client.list_tables(request)
        return ListTablesResponse(
            tables=response.tables if response.tables else [],
            page_token=response.page_token,
        )

    async def namespace_client(self) -> LanceNamespace:
        """Get the namespace client for this connection.

        For namespace connections, this returns the backing namespace client
        that was provided during construction.

        Returns
        -------
        LanceNamespace
            The namespace client for this connection.
        """
        return self._namespace_client


def connect_namespace(
    namespace_client_impl: str,
    namespace_client_properties: Dict[str, str],
    *,
    read_consistency_interval: Optional[timedelta] = None,
    storage_options: Optional[Dict[str, str]] = None,
    session: Optional[Session] = None,
    namespace_client_pushdown_operations: Optional[List[str]] = None,
) -> LanceNamespaceDBConnection:
    """
    Connect to a LanceDB database through a namespace.

    Parameters
    ----------
    namespace_client_impl : str
        The namespace client implementation to use. For examples:

        - "dir" for DirectoryNamespace
        - "rest" for REST-based namespace
        - Full module path for custom implementations
    namespace_client_properties : Dict[str, str]
        Configuration properties for the namespace client implementation.
        Different namespace implementations have different config properties.
        For example, use DirectoryNamespace with {"root": "/path/to/directory"}
    read_consistency_interval : Optional[timedelta]
        The interval at which to check for updates to the table from other
        processes. If None, then consistency is not checked.
    storage_options : Optional[Dict[str, str]]
        Additional options for the storage backend
    session : Optional[Session]
        A session to use for this connection
    namespace_client_pushdown_operations : Optional[List[str]]
        List of namespace operations to push down to the namespace server.
        Supported values:

        - "QueryTable": Execute queries on the namespace server via
          namespace.query_table() instead of locally.
        - "CreateTable": Execute table creation on the namespace server via
          namespace.create_table() instead of using declare_table + local write.

        Default is None (no pushdown, all operations run locally).

    Returns
    -------
    LanceNamespaceDBConnection
        A namespace-based connection to LanceDB
    """
    namespace_client = namespace_connect(
        namespace_client_impl, namespace_client_properties
    )

    return LanceNamespaceDBConnection(
        namespace_client=namespace_client,
        read_consistency_interval=read_consistency_interval,
        storage_options=storage_options,
        session=session,
        namespace_client_pushdown_operations=namespace_client_pushdown_operations,
        namespace_client_impl=namespace_client_impl,
        namespace_client_properties=namespace_client_properties,
    )


def connect_namespace_async(
    namespace_client_impl: str,
    namespace_client_properties: Dict[str, str],
    *,
    read_consistency_interval: Optional[timedelta] = None,
    storage_options: Optional[Dict[str, str]] = None,
    session: Optional[Session] = None,
    namespace_client_pushdown_operations: Optional[List[str]] = None,
) -> AsyncLanceNamespaceDBConnection:
    """
    Connect to a LanceDB database through a namespace (returns async connection).

    This function is synchronous but returns an AsyncLanceNamespaceDBConnection
    that provides async methods for all database operations.

    Parameters
    ----------
    namespace_client_impl : str
        The namespace client implementation to use. For examples:

        - "dir" for DirectoryNamespace
        - "rest" for REST-based namespace
        - Full module path for custom implementations
    namespace_client_properties : Dict[str, str]
        Configuration properties for the namespace client implementation.
        Different namespace implementations have different config properties.
        For example, use DirectoryNamespace with {"root": "/path/to/directory"}
    read_consistency_interval : Optional[timedelta]
        The interval at which to check for updates to the table from other
        processes. If None, then consistency is not checked.
    storage_options : Optional[Dict[str, str]]
        Additional options for the storage backend
    session : Optional[Session]
        A session to use for this connection
    namespace_client_pushdown_operations : Optional[List[str]]
        List of namespace operations to push down to the namespace server.
        Supported values:

        - "QueryTable": Execute queries on the namespace server via
          namespace.query_table() instead of locally.
        - "CreateTable": Execute table creation on the namespace server via
          namespace.create_table() instead of using declare_table + local write.

        Default is None (no pushdown, all operations run locally).

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
    namespace_client = namespace_connect(
        namespace_client_impl, namespace_client_properties
    )

    return AsyncLanceNamespaceDBConnection(
        namespace_client=namespace_client,
        read_consistency_interval=read_consistency_interval,
        storage_options=storage_options,
        session=session,
        namespace_client_pushdown_operations=namespace_client_pushdown_operations,
    )
