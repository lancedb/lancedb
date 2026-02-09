# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors


from datetime import timedelta
import logging
from concurrent.futures import ThreadPoolExecutor
import sys
from typing import Any, Dict, Iterable, List, Optional, Union
from urllib.parse import urlparse
import warnings

if sys.version_info >= (3, 12):
    from typing import override
else:
    from overrides import override

# Remove this import to fix circular dependency
# from lancedb import connect_async
from lancedb.remote import ClientConfig
import pyarrow as pa

from ..common import DATA
from ..db import DBConnection, LOOP
from ..embeddings import EmbeddingFunctionConfig
from lance_namespace import (
    CreateNamespaceResponse,
    DescribeNamespaceResponse,
    DropNamespaceResponse,
    ListNamespacesResponse,
    ListTablesResponse,
)
from ..pydantic import LanceModel
from ..table import Table
from ..util import validate_table_name


class RemoteDBConnection(DBConnection):
    """A connection to a remote LanceDB database."""

    def __init__(
        self,
        db_url: str,
        api_key: str,
        region: str,
        host_override: Optional[str] = None,
        request_thread_pool: Optional[ThreadPoolExecutor] = None,
        client_config: Union[ClientConfig, Dict[str, Any], None] = None,
        connection_timeout: Optional[float] = None,
        read_timeout: Optional[float] = None,
        storage_options: Optional[Dict[str, str]] = None,
    ):
        """Connect to a remote LanceDB database."""
        if isinstance(client_config, dict):
            client_config = ClientConfig(**client_config)
        elif client_config is None:
            client_config = ClientConfig()

        # These are legacy options from the old Python-based client. We keep them
        # here for backwards compatibility, but will remove them in a future release.
        if request_thread_pool is not None:
            warnings.warn(
                "request_thread_pool is no longer used and will be removed in "
                "a future release.",
                DeprecationWarning,
            )

        if connection_timeout is not None:
            warnings.warn(
                "connection_timeout is deprecated and will be removed in a future "
                "release. Please use client_config.timeout_config.connect_timeout "
                "instead.",
                DeprecationWarning,
            )
            client_config.timeout_config.connect_timeout = timedelta(
                seconds=connection_timeout
            )

        if read_timeout is not None:
            warnings.warn(
                "read_timeout is deprecated and will be removed in a future release. "
                "Please use client_config.timeout_config.read_timeout instead.",
                DeprecationWarning,
            )
            client_config.timeout_config.read_timeout = timedelta(seconds=read_timeout)

        parsed = urlparse(db_url)
        if parsed.scheme != "db":
            raise ValueError(f"Invalid scheme: {parsed.scheme}, only accepts db://")
        self.db_name = parsed.netloc

        self.client_config = client_config

        # Import connect_async here to avoid circular import
        from lancedb import connect_async

        self._conn = LOOP.run(
            connect_async(
                db_url,
                api_key=api_key,
                region=region,
                host_override=host_override,
                client_config=client_config,
                storage_options=storage_options,
            )
        )

    def __repr__(self) -> str:
        return f"RemoteConnect(name={self.db_name})"

    @override
    def list_namespaces(
        self,
        namespace: Optional[List[str]] = None,
        page_token: Optional[str] = None,
        limit: Optional[int] = None,
    ) -> ListNamespacesResponse:
        """List immediate child namespace names in the given namespace.

        Parameters
        ----------
        namespace: List[str], optional
            The parent namespace to list namespaces in.
            None or empty list represents root namespace.
        page_token: str, optional
            Token for pagination. Use the token from a previous response
            to get the next page of results.
        limit: int, optional
            The maximum number of results to return.

        Returns
        -------
        ListNamespacesResponse
            Response containing namespace names and optional page_token for pagination.
        """
        if namespace is None:
            namespace = []
        return LOOP.run(
            self._conn.list_namespaces(
                namespace=namespace, page_token=page_token, limit=limit
            )
        )

    @override
    def create_namespace(
        self,
        namespace: List[str],
        mode: Optional[str] = None,
        properties: Optional[Dict[str, str]] = None,
    ) -> CreateNamespaceResponse:
        """Create a new namespace.

        Parameters
        ----------
        namespace: List[str]
            The namespace identifier to create.
        mode: str, optional
            Creation mode - "create" (fail if exists), "exist_ok" (skip if exists),
            or "overwrite" (replace if exists). Case insensitive.
        properties: Dict[str, str], optional
            Properties to set on the namespace.

        Returns
        -------
        CreateNamespaceResponse
            Response containing the properties of the created namespace.
        """
        return LOOP.run(
            self._conn.create_namespace(
                namespace=namespace, mode=mode, properties=properties
            )
        )

    @override
    def drop_namespace(
        self,
        namespace: List[str],
        mode: Optional[str] = None,
        behavior: Optional[str] = None,
    ) -> DropNamespaceResponse:
        """Drop a namespace.

        Parameters
        ----------
        namespace: List[str]
            The namespace identifier to drop.
        mode: str, optional
            Whether to skip if not exists ("SKIP") or fail ("FAIL"). Case insensitive.
        behavior: str, optional
            Whether to restrict drop if not empty ("RESTRICT") or cascade ("CASCADE").
            Case insensitive.

        Returns
        -------
        DropNamespaceResponse
            Response containing properties and transaction_id if applicable.
        """
        return LOOP.run(
            self._conn.drop_namespace(namespace=namespace, mode=mode, behavior=behavior)
        )

    @override
    def describe_namespace(self, namespace: List[str]) -> DescribeNamespaceResponse:
        """Describe a namespace.

        Parameters
        ----------
        namespace: List[str]
            The namespace identifier to describe.

        Returns
        -------
        DescribeNamespaceResponse
            Response containing the namespace properties.
        """
        return LOOP.run(self._conn.describe_namespace(namespace=namespace))

    @override
    def list_tables(
        self,
        namespace: Optional[List[str]] = None,
        page_token: Optional[str] = None,
        limit: Optional[int] = None,
    ) -> ListTablesResponse:
        """List all tables in this database with pagination support.

        Parameters
        ----------
        namespace: List[str], optional
            The namespace to list tables in.
            None or empty list represents root namespace.
        page_token: str, optional
            Token for pagination. Use the token from a previous response
            to get the next page of results.
        limit: int, optional
            The maximum number of results to return.

        Returns
        -------
        ListTablesResponse
            Response containing table names and optional page_token for pagination.
        """
        if namespace is None:
            namespace = []
        return LOOP.run(
            self._conn.list_tables(
                namespace=namespace, page_token=page_token, limit=limit
            )
        )

    @override
    def table_names(
        self,
        page_token: Optional[str] = None,
        limit: int = 10,
        *,
        namespace: Optional[List[str]] = None,
    ) -> Iterable[str]:
        """List the names of all tables in the database.

        .. deprecated::
            Use :meth:`list_tables` instead, which provides proper pagination support.

        Parameters
        ----------
        namespace: List[str], default []
            The namespace to list tables in.
            Empty list represents root namespace.
        page_token: str
            The last token to start the new page.
        limit: int, default 10
            The maximum number of tables to return for each page.

        Returns
        -------
        An iterator of table names.
        """
        import warnings

        warnings.warn(
            "table_names() is deprecated, use list_tables() instead",
            DeprecationWarning,
            stacklevel=2,
        )
        if namespace is None:
            namespace = []
        return LOOP.run(
            self._conn.table_names(
                namespace=namespace, start_after=page_token, limit=limit
            )
        )

    @override
    def open_table(
        self,
        name: str,
        *,
        namespace: Optional[List[str]] = None,
        storage_options: Optional[Dict[str, str]] = None,
        index_cache_size: Optional[int] = None,
    ) -> Table:
        """Open a Lance Table in the database.

        Parameters
        ----------
        name: str
            The name of the table.
        namespace: List[str], optional
            The namespace to open the table from.
            None or empty list represents root namespace.

        Returns
        -------
        A LanceTable object representing the table.
        """
        from .table import RemoteTable

        if namespace is None:
            namespace = []
        if index_cache_size is not None:
            logging.info(
                "index_cache_size is ignored in LanceDb Cloud"
                " (there is no local cache to configure)"
            )

        table = LOOP.run(self._conn.open_table(name, namespace=namespace))
        return RemoteTable(table, self.db_name)

    def clone_table(
        self,
        target_table_name: str,
        source_uri: str,
        *,
        target_namespace: Optional[List[str]] = None,
        source_version: Optional[int] = None,
        source_tag: Optional[str] = None,
        is_shallow: bool = True,
    ) -> Table:
        """Clone a table from a source table.

        Parameters
        ----------
        target_table_name: str
            The name of the target table to create.
        source_uri: str
            The URI of the source table to clone from.
        target_namespace: List[str], optional
            The namespace for the target table.
            None or empty list represents root namespace.
        source_version: int, optional
            The version of the source table to clone.
        source_tag: str, optional
            The tag of the source table to clone.
        is_shallow: bool, default True
            Whether to perform a shallow clone (True) or deep clone (False).
            Currently only shallow clone is supported.

        Returns
        -------
        A RemoteTable object representing the cloned table.
        """
        from .table import RemoteTable

        if target_namespace is None:
            target_namespace = []
        table = LOOP.run(
            self._conn.clone_table(
                target_table_name,
                source_uri,
                target_namespace=target_namespace,
                source_version=source_version,
                source_tag=source_tag,
                is_shallow=is_shallow,
            )
        )
        return RemoteTable(table, self.db_name)

    @override
    def create_table(
        self,
        name: str,
        data: DATA = None,
        schema: Optional[Union[pa.Schema, LanceModel]] = None,
        on_bad_vectors: str = "error",
        fill_value: float = 0.0,
        mode: Optional[str] = None,
        exist_ok: bool = False,
        embedding_functions: Optional[List[EmbeddingFunctionConfig]] = None,
        *,
        namespace: Optional[List[str]] = None,
    ) -> Table:
        """Create a [Table][lancedb.table.Table] in the database.

        Parameters
        ----------
        name: str
            The name of the table.
        namespace: List[str], optional
            The namespace to create the table in.
            None or empty list represents root namespace.
        data: The data to initialize the table, *optional*
            User must provide at least one of `data` or `schema`.
            Acceptable types are:

            - dict or list-of-dict

            - pandas.DataFrame

            - pyarrow.Table or pyarrow.RecordBatch
        schema: The schema of the table, *optional*
            Acceptable types are:

            - pyarrow.Schema

            - [LanceModel][lancedb.pydantic.LanceModel]
        mode: str, default "create"
            The mode to use when creating the table.
            Can be either "create", "overwrite", or "exist_ok".
        exist_ok: bool, default False
            If exist_ok is True, and mode is None or "create", mode will be changed
            to "exist_ok".
        on_bad_vectors: str, default "error"
            What to do if any of the vectors are not the same size or contains NaNs.
            One of "error", "drop", "fill".
        fill_value: float
            The value to use when filling vectors. Only used if on_bad_vectors="fill".

        Returns
        -------
        LanceTable
            A reference to the newly created table.

        !!! note

            The vector index won't be created by default.
            To create the index, call the `create_index` method on the table.

        Examples
        --------

        Can create with list of tuples or dictionaries:

        >>> import lancedb
        >>> db = lancedb.connect("db://...", api_key="...", # doctest: +SKIP
        ...                      region="...")              # doctest: +SKIP
        >>> data = [{"vector": [1.1, 1.2], "lat": 45.5, "long": -122.7},
        ...         {"vector": [0.2, 1.8], "lat": 40.1, "long":  -74.1}]
        >>> db.create_table("my_table", data) # doctest: +SKIP
        LanceTable(my_table)

        You can also pass a pandas DataFrame:

        >>> import pandas as pd
        >>> data = pd.DataFrame({
        ...    "vector": [[1.1, 1.2], [0.2, 1.8]],
        ...    "lat": [45.5, 40.1],
        ...    "long": [-122.7, -74.1]
        ... })
        >>> db.create_table("table2", data) # doctest: +SKIP
        LanceTable(table2)

        >>> custom_schema = pa.schema([
        ...   pa.field("vector", pa.list_(pa.float32(), 2)),
        ...   pa.field("lat", pa.float32()),
        ...   pa.field("long", pa.float32())
        ... ])
        >>> db.create_table("table3", data, schema = custom_schema) # doctest: +SKIP
        LanceTable(table3)

        It is also possible to create an table from `[Iterable[pa.RecordBatch]]`:

        >>> import pyarrow as pa
        >>> def make_batches():
        ...     for i in range(5):
        ...         yield pa.RecordBatch.from_arrays(
        ...             [
        ...                 pa.array([[3.1, 4.1], [5.9, 26.5]],
        ...                     pa.list_(pa.float32(), 2)),
        ...                 pa.array(["foo", "bar"]),
        ...                 pa.array([10.0, 20.0]),
        ...             ],
        ...             ["vector", "item", "price"],
        ...         )
        >>> schema=pa.schema([
        ...     pa.field("vector", pa.list_(pa.float32(), 2)),
        ...     pa.field("item", pa.utf8()),
        ...     pa.field("price", pa.float32()),
        ... ])
        >>> db.create_table("table4", make_batches(), schema=schema) # doctest: +SKIP
        LanceTable(table4)

        """
        if exist_ok:
            if mode == "create":
                mode = "exist_ok"
            elif not mode:
                mode = "exist_ok"
        if namespace is None:
            namespace = []
        validate_table_name(name)
        if embedding_functions is not None:
            logging.warning(
                "embedding_functions is not yet supported on LanceDB Cloud."
                "Please vote https://github.com/lancedb/lancedb/issues/626 "
                "for this feature."
            )

        from .table import RemoteTable

        table = LOOP.run(
            self._conn.create_table(
                name,
                data,
                namespace=namespace,
                mode=mode,
                schema=schema,
                on_bad_vectors=on_bad_vectors,
                fill_value=fill_value,
            )
        )
        return RemoteTable(table, self.db_name)

    @override
    def drop_table(self, name: str, namespace: Optional[List[str]] = None):
        """Drop a table from the database.

        Parameters
        ----------
        name: str
            The name of the table.
        namespace: List[str], optional
            The namespace to drop the table from.
            None or empty list represents root namespace.
        """
        if namespace is None:
            namespace = []
        LOOP.run(self._conn.drop_table(name, namespace=namespace))

    @override
    def rename_table(
        self,
        cur_name: str,
        new_name: str,
        cur_namespace: Optional[List[str]] = None,
        new_namespace: Optional[List[str]] = None,
    ):
        """Rename a table in the database.

        Parameters
        ----------
        cur_name: str
            The current name of the table.
        new_name: str
            The new name of the table.
        """
        if cur_namespace is None:
            cur_namespace = []
        if new_namespace is None:
            new_namespace = []
        LOOP.run(
            self._conn.rename_table(
                cur_name,
                new_name,
                cur_namespace=cur_namespace,
                new_namespace=new_namespace,
            )
        )

    async def close(self):
        """Close the connection to the database."""
        self._client.close()
