# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors


from __future__ import annotations

from abc import abstractmethod
from pathlib import Path
from typing import TYPE_CHECKING, Dict, Iterable, List, Literal, Optional, Union

from lancedb.embeddings.registry import EmbeddingFunctionRegistry
from overrides import EnforceOverrides, override  # type: ignore

from lancedb.common import data_to_reader, sanitize_uri, validate_schema
from lancedb.background_loop import LOOP

from . import __version__
from ._lancedb import connect as lancedb_connect  # type: ignore
from .table import (
    AsyncTable,
    LanceTable,
    Table,
    sanitize_create_table,
)
from .util import (
    get_uri_scheme,
    validate_table_name,
)

import deprecation

if TYPE_CHECKING:
    import pyarrow as pa
    from .pydantic import LanceModel
    from datetime import timedelta

    from ._lancedb import Connection as LanceDbConnection
    from .common import DATA, URI
    from .embeddings import EmbeddingFunctionConfig
    from ._lancedb import Session


class DBConnection(EnforceOverrides):
    """An active LanceDB connection interface."""

    @abstractmethod
    def table_names(
        self, page_token: Optional[str] = None, limit: int = 10
    ) -> Iterable[str]:
        """List all tables in this database, in sorted order

        Parameters
        ----------
        page_token: str, optional
            The token to use for pagination. If not present, start from the beginning.
            Typically, this token is last table name from the previous page.
            Only supported by LanceDb Cloud.
        limit: int, default 10
            The size of the page to return.
            Only supported by LanceDb Cloud.

        Returns
        -------
        Iterable of str
        """
        pass

    @abstractmethod
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
        """Create a [Table][lancedb.table.Table] in the database.

        Parameters
        ----------
        name: str
            The name of the table.
        data: The data to initialize the table, *optional*
            User must provide at least one of `data` or `schema`.
            Acceptable types are:

            - list-of-dict

            - pandas.DataFrame

            - pyarrow.Table or pyarrow.RecordBatch
        schema: The schema of the table, *optional*
            Acceptable types are:

            - pyarrow.Schema

            - [LanceModel][lancedb.pydantic.LanceModel]
        mode: str; default "create"
            The mode to use when creating the table.
            Can be either "create" or "overwrite".
            By default, if the table already exists, an exception is raised.
            If you want to overwrite the table, use mode="overwrite".
        exist_ok: bool, default False
            If a table by the same name already exists, then raise an exception
            if exist_ok=False. If exist_ok=True, then open the existing table;
            it will not add the provided data but will validate against any
            schema that's specified.
        on_bad_vectors: str, default "error"
            What to do if any of the vectors are not the same size or contains NaNs.
            One of "error", "drop", "fill".
        fill_value: float
            The value to use when filling vectors. Only used if on_bad_vectors="fill".
        storage_options: dict, optional
            Additional options for the storage backend. Options already set on the
            connection will be inherited by the table, but can be overridden here.
            See available options at
            <https://lancedb.github.io/lancedb/guides/storage/>
        data_storage_version: optional, str, default "stable"
            Deprecated.  Set `storage_options` when connecting to the database and set
            `new_table_data_storage_version` in the options.
        enable_v2_manifest_paths: optional, bool, default False
            Deprecated.  Set `storage_options` when connecting to the database and set
            `new_table_enable_v2_manifest_paths` in the options.
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
        >>> db = lancedb.connect("./.lancedb")
        >>> data = [{"vector": [1.1, 1.2], "lat": 45.5, "long": -122.7},
        ...         {"vector": [0.2, 1.8], "lat": 40.1, "long":  -74.1}]
        >>> db.create_table("my_table", data)
        LanceTable(name='my_table', version=1, ...)
        >>> db["my_table"].head()
        pyarrow.Table
        vector: fixed_size_list<item: float>[2]
          child 0, item: float
        lat: double
        long: double
        ----
        vector: [[[1.1,1.2],[0.2,1.8]]]
        lat: [[45.5,40.1]]
        long: [[-122.7,-74.1]]

        You can also pass a pandas DataFrame:

        >>> import pandas as pd
        >>> data = pd.DataFrame({
        ...    "vector": [[1.1, 1.2], [0.2, 1.8]],
        ...    "lat": [45.5, 40.1],
        ...    "long": [-122.7, -74.1]
        ... })
        >>> db.create_table("table2", data)
        LanceTable(name='table2', version=1, ...)
        >>> db["table2"].head()
        pyarrow.Table
        vector: fixed_size_list<item: float>[2]
          child 0, item: float
        lat: double
        long: double
        ----
        vector: [[[1.1,1.2],[0.2,1.8]]]
        lat: [[45.5,40.1]]
        long: [[-122.7,-74.1]]

        Data is converted to Arrow before being written to disk. For maximum
        control over how data is saved, either provide the PyArrow schema to
        convert to or else provide a [PyArrow Table](pyarrow.Table) directly.

        >>> import pyarrow as pa
        >>> custom_schema = pa.schema([
        ...   pa.field("vector", pa.list_(pa.float32(), 2)),
        ...   pa.field("lat", pa.float32()),
        ...   pa.field("long", pa.float32())
        ... ])
        >>> db.create_table("table3", data, schema = custom_schema)
        LanceTable(name='table3', version=1, ...)
        >>> db["table3"].head()
        pyarrow.Table
        vector: fixed_size_list<item: float>[2]
          child 0, item: float
        lat: float
        long: float
        ----
        vector: [[[1.1,1.2],[0.2,1.8]]]
        lat: [[45.5,40.1]]
        long: [[-122.7,-74.1]]


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
        >>> db.create_table("table4", make_batches(), schema=schema)
        LanceTable(name='table4', version=1, ...)

        """
        raise NotImplementedError

    def __getitem__(self, name: str) -> LanceTable:
        return self.open_table(name)

    def open_table(
        self,
        name: str,
        *,
        storage_options: Optional[Dict[str, str]] = None,
        index_cache_size: Optional[int] = None,
    ) -> Table:
        """Open a Lance Table in the database.

        Parameters
        ----------
        name: str
            The name of the table.
        index_cache_size: int, default 256
            **Deprecated**: Use session-level cache configuration instead.
            Create a Session with custom cache sizes and pass it to lancedb.connect().

            Set the size of the index cache, specified as a number of entries

            The exact meaning of an "entry" will depend on the type of index:
            * IVF - there is one entry for each IVF partition
            * BTREE - there is one entry for the entire index

            This cache applies to the entire opened table, across all indices.
            Setting this value higher will increase performance on larger datasets
            at the expense of more RAM
        storage_options: dict, optional
            Additional options for the storage backend. Options already set on the
            connection will be inherited by the table, but can be overridden here.
            See available options at
            <https://lancedb.github.io/lancedb/guides/storage/>

        Returns
        -------
        A LanceTable object representing the table.
        """
        raise NotImplementedError

    def drop_table(self, name: str):
        """Drop a table from the database.

        Parameters
        ----------
        name: str
            The name of the table.
        """
        raise NotImplementedError

    def rename_table(self, cur_name: str, new_name: str):
        """Rename a table in the database.

        Parameters
        ----------
        cur_name: str
            The current name of the table.
        new_name: str
            The new name of the table.
        """
        raise NotImplementedError

    def drop_database(self):
        """
        Drop database
        This is the same thing as dropping all the tables
        """
        raise NotImplementedError

    def drop_all_tables(self):
        """
        Drop all tables from the database
        """
        raise NotImplementedError

    @property
    def uri(self) -> str:
        return self._uri


class LanceDBConnection(DBConnection):
    """
    A connection to a LanceDB database.

    Parameters
    ----------
    uri: str or Path
        The root uri of the database.
    read_consistency_interval: timedelta, default None
        The interval at which to check for updates to the table from other
        processes. If None, then consistency is not checked. For performance
        reasons, this is the default. For strong consistency, set this to
        zero seconds. Then every read will check for updates from other
        processes. As a compromise, you can set this to a non-zero timedelta
        for eventual consistency. If more than that interval has passed since
        the last check, then the table will be checked for updates. Note: this
        consistency only applies to read operations. Write operations are
        always consistent.

    Examples
    --------
    >>> import lancedb
    >>> db = lancedb.connect("./.lancedb")
    >>> db.create_table("my_table", data=[{"vector": [1.1, 1.2], "b": 2},
    ...                                   {"vector": [0.5, 1.3], "b": 4}])
    LanceTable(name='my_table', version=1, ...)
    >>> db.create_table("another_table", data=[{"vector": [0.4, 0.4], "b": 6}])
    LanceTable(name='another_table', version=1, ...)
    >>> sorted(db.table_names())
    ['another_table', 'my_table']
    >>> len(db)
    2
    >>> db["my_table"]
    LanceTable(name='my_table', version=1, ...)
    >>> "my_table" in db
    True
    >>> db.drop_table("my_table")
    >>> db.drop_table("another_table")
    """

    def __init__(
        self,
        uri: URI,
        *,
        read_consistency_interval: Optional[timedelta] = None,
        storage_options: Optional[Dict[str, str]] = None,
        session: Optional[Session] = None,
    ):
        if not isinstance(uri, Path):
            scheme = get_uri_scheme(uri)
        is_local = isinstance(uri, Path) or scheme == "file"
        if is_local:
            if isinstance(uri, str):
                uri = Path(uri)
            uri = uri.expanduser().absolute()
            Path(uri).mkdir(parents=True, exist_ok=True)
        self._uri = str(uri)
        self._entered = False
        self.read_consistency_interval = read_consistency_interval
        self.storage_options = storage_options
        self.session = session

        if read_consistency_interval is not None:
            read_consistency_interval_secs = read_consistency_interval.total_seconds()
        else:
            read_consistency_interval_secs = None

        async def do_connect():
            return await lancedb_connect(
                sanitize_uri(uri),
                None,
                None,
                None,
                read_consistency_interval_secs,
                None,
                storage_options,
                session,
            )

        self._conn = AsyncConnection(LOOP.run(do_connect()))

    def __repr__(self) -> str:
        val = f"{self.__class__.__name__}(uri={self._uri!r}"
        if self.read_consistency_interval is not None:
            val += f", read_consistency_interval={repr(self.read_consistency_interval)}"
        val += ")"
        return val

    async def _async_get_table_names(self, start_after: Optional[str], limit: int):
        conn = AsyncConnection(await lancedb_connect(self.uri))
        return await conn.table_names(start_after=start_after, limit=limit)

    @override
    def table_names(
        self, page_token: Optional[str] = None, limit: int = 10
    ) -> Iterable[str]:
        """Get the names of all tables in the database. The names are sorted.

        Returns
        -------
        Iterator of str.
            A list of table names.
        """
        return LOOP.run(self._conn.table_names(start_after=page_token, limit=limit))

    def __len__(self) -> int:
        return len(self.table_names())

    def __contains__(self, name: str) -> bool:
        return name in self.table_names()

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
    ) -> LanceTable:
        """Create a table in the database.

        See
        ---
        DBConnection.create_table
        """
        if mode.lower() not in ["create", "overwrite"]:
            raise ValueError("mode must be either 'create' or 'overwrite'")
        validate_table_name(name)

        tbl = LanceTable.create(
            self,
            name,
            data,
            schema,
            mode=mode,
            exist_ok=exist_ok,
            on_bad_vectors=on_bad_vectors,
            fill_value=fill_value,
            embedding_functions=embedding_functions,
            storage_options=storage_options,
        )
        return tbl

    @override
    def open_table(
        self,
        name: str,
        *,
        storage_options: Optional[Dict[str, str]] = None,
        index_cache_size: Optional[int] = None,
    ) -> LanceTable:
        """Open a table in the database.

        Parameters
        ----------
        name: str
            The name of the table.

        Returns
        -------
        A LanceTable object representing the table.
        """
        if index_cache_size is not None:
            import warnings

            warnings.warn(
                "index_cache_size is deprecated. Use session-level cache "
                "configuration instead. Create a Session with custom cache sizes "
                "and pass it to lancedb.connect().",
                DeprecationWarning,
                stacklevel=2,
            )

        return LanceTable.open(
            self,
            name,
            storage_options=storage_options,
            index_cache_size=index_cache_size,
        )

    @override
    def drop_table(self, name: str, ignore_missing: bool = False):
        """Drop a table from the database.

        Parameters
        ----------
        name: str
            The name of the table.
        ignore_missing: bool, default False
            If True, ignore if the table does not exist.
        """
        LOOP.run(self._conn.drop_table(name, ignore_missing=ignore_missing))

    @override
    def drop_all_tables(self):
        LOOP.run(self._conn.drop_all_tables())

    @deprecation.deprecated(
        deprecated_in="0.15.1",
        removed_in="0.17",
        current_version=__version__,
        details="Use drop_all_tables() instead",
    )
    @override
    def drop_database(self):
        LOOP.run(self._conn.drop_all_tables())


class AsyncConnection(object):
    """An active LanceDB connection

    To obtain a connection you can use the [connect_async][lancedb.connect_async]
    function.

    This could be a native connection (using lance) or a remote connection (e.g. for
    connecting to LanceDb Cloud)

    Local connections do not currently hold any open resources but they may do so in the
    future (for example, for shared cache or connections to catalog services) Remote
    connections represent an open connection to the remote server.  The
    [close][lancedb.db.AsyncConnection.close] method can be used to release any
    underlying resources eagerly.  The connection can also be used as a context manager.

    Connections can be shared on multiple threads and are expected to be long lived.
    Connections can also be used as a context manager, however, in many cases a single
    connection can be used for the lifetime of the application and so this is often
    not needed.  Closing a connection is optional.  If it is not closed then it will
    be automatically closed when the connection object is deleted.

    Examples
    --------

    >>> import lancedb
    >>> async def doctest_example():
    ...   with await lancedb.connect_async("/tmp/my_dataset") as conn:
    ...     # do something with the connection
    ...     pass
    ...   # conn is closed here
    """

    def __init__(self, connection: LanceDbConnection):
        self._inner = connection

    def __repr__(self):
        return self._inner.__repr__()

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.close()

    def is_open(self):
        """Return True if the connection is open."""
        return self._inner.is_open()

    def close(self):
        """Close the connection, releasing any underlying resources.

        It is safe to call this method multiple times.

        Any attempt to use the connection after it is closed will result in an error."""
        self._inner.close()

    @property
    def uri(self) -> str:
        return self._inner.uri

    async def table_names(
        self, *, start_after: Optional[str] = None, limit: Optional[int] = None
    ) -> Iterable[str]:
        """List all tables in this database, in sorted order

        Parameters
        ----------
        start_after: str, optional
            If present, only return names that come lexicographically after the supplied
            value.

            This can be combined with limit to implement pagination by setting this to
            the last table name from the previous page.
        limit: int, default 10
            The number of results to return.

        Returns
        -------
        Iterable of str
        """
        return await self._inner.table_names(start_after=start_after, limit=limit)

    async def create_table(
        self,
        name: str,
        data: Optional[DATA] = None,
        schema: Optional[Union[pa.Schema, LanceModel]] = None,
        mode: Optional[Literal["create", "overwrite"]] = None,
        exist_ok: Optional[bool] = None,
        on_bad_vectors: Optional[str] = None,
        fill_value: Optional[float] = None,
        storage_options: Optional[Dict[str, str]] = None,
        *,
        embedding_functions: Optional[List[EmbeddingFunctionConfig]] = None,
    ) -> AsyncTable:
        """Create an [AsyncTable][lancedb.table.AsyncTable] in the database.

        Parameters
        ----------
        name: str
            The name of the table.
        data: The data to initialize the table, *optional*
            User must provide at least one of `data` or `schema`.
            Acceptable types are:

            - list-of-dict

            - pandas.DataFrame

            - pyarrow.Table or pyarrow.RecordBatch
        schema: The schema of the table, *optional*
            Acceptable types are:

            - pyarrow.Schema

            - [LanceModel][lancedb.pydantic.LanceModel]
        mode: Literal["create", "overwrite"]; default "create"
            The mode to use when creating the table.
            Can be either "create" or "overwrite".
            By default, if the table already exists, an exception is raised.
            If you want to overwrite the table, use mode="overwrite".
        exist_ok: bool, default False
            If a table by the same name already exists, then raise an exception
            if exist_ok=False. If exist_ok=True, then open the existing table;
            it will not add the provided data but will validate against any
            schema that's specified.
        on_bad_vectors: str, default "error"
            What to do if any of the vectors are not the same size or contains NaNs.
            One of "error", "drop", "fill".
        fill_value: float
            The value to use when filling vectors. Only used if on_bad_vectors="fill".
        storage_options: dict, optional
            Additional options for the storage backend. Options already set on the
            connection will be inherited by the table, but can be overridden here.
            See available options at
            <https://lancedb.github.io/lancedb/guides/storage/>

        Returns
        -------
        AsyncTable
            A reference to the newly created table.

        !!! note

            The vector index won't be created by default.
            To create the index, call the `create_index` method on the table.

        Examples
        --------

        Can create with list of tuples or dictionaries:

        >>> import lancedb
        >>> async def doctest_example():
        ...     db = await lancedb.connect_async("./.lancedb")
        ...     data = [{"vector": [1.1, 1.2], "lat": 45.5, "long": -122.7},
        ...             {"vector": [0.2, 1.8], "lat": 40.1, "long":  -74.1}]
        ...     my_table = await db.create_table("my_table", data)
        ...     print(await my_table.query().limit(5).to_arrow())
        >>> import asyncio
        >>> asyncio.run(doctest_example())
        pyarrow.Table
        vector: fixed_size_list<item: float>[2]
          child 0, item: float
        lat: double
        long: double
        ----
        vector: [[[1.1,1.2],[0.2,1.8]]]
        lat: [[45.5,40.1]]
        long: [[-122.7,-74.1]]

        You can also pass a pandas DataFrame:

        >>> import pandas as pd
        >>> data = pd.DataFrame({
        ...    "vector": [[1.1, 1.2], [0.2, 1.8]],
        ...    "lat": [45.5, 40.1],
        ...    "long": [-122.7, -74.1]
        ... })
        >>> async def pandas_example():
        ...     db = await lancedb.connect_async("./.lancedb")
        ...     my_table = await db.create_table("table2", data)
        ...     print(await my_table.query().limit(5).to_arrow())
        >>> asyncio.run(pandas_example())
        pyarrow.Table
        vector: fixed_size_list<item: float>[2]
          child 0, item: float
        lat: double
        long: double
        ----
        vector: [[[1.1,1.2],[0.2,1.8]]]
        lat: [[45.5,40.1]]
        long: [[-122.7,-74.1]]

        Data is converted to Arrow before being written to disk. For maximum
        control over how data is saved, either provide the PyArrow schema to
        convert to or else provide a [PyArrow Table](pyarrow.Table) directly.

        >>> import pyarrow as pa
        >>> custom_schema = pa.schema([
        ...   pa.field("vector", pa.list_(pa.float32(), 2)),
        ...   pa.field("lat", pa.float32()),
        ...   pa.field("long", pa.float32())
        ... ])
        >>> async def with_schema():
        ...     db = await lancedb.connect_async("./.lancedb")
        ...     my_table = await db.create_table("table3", data, schema = custom_schema)
        ...     print(await my_table.query().limit(5).to_arrow())
        >>> asyncio.run(with_schema())
        pyarrow.Table
        vector: fixed_size_list<item: float>[2]
          child 0, item: float
        lat: float
        long: float
        ----
        vector: [[[1.1,1.2],[0.2,1.8]]]
        lat: [[45.5,40.1]]
        long: [[-122.7,-74.1]]


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
        >>> async def iterable_example():
        ...     db = await lancedb.connect_async("./.lancedb")
        ...     await db.create_table("table4", make_batches(), schema=schema)
        >>> asyncio.run(iterable_example())
        """
        metadata = None

        if embedding_functions is not None:
            # If we passed in embedding functions explicitly
            # then we'll override any schema metadata that
            # may was implicitly specified by the LanceModel schema
            registry = EmbeddingFunctionRegistry.get_instance()
            metadata = registry.get_table_metadata(embedding_functions)

        # Defining defaults here and not in function prototype.  In the future
        # these defaults will move into rust so better to keep them as None.
        if on_bad_vectors is None:
            on_bad_vectors = "error"

        if fill_value is None:
            fill_value = 0.0

        data, schema = sanitize_create_table(
            data, schema, metadata, on_bad_vectors, fill_value
        )
        validate_schema(schema)

        if exist_ok is None:
            exist_ok = False
        if mode is None:
            mode = "create"
        if mode == "create" and exist_ok:
            mode = "exist_ok"

        if data is None:
            new_table = await self._inner.create_empty_table(
                name,
                mode,
                schema,
                storage_options=storage_options,
            )
        else:
            data = data_to_reader(data, schema)
            new_table = await self._inner.create_table(
                name,
                mode,
                data,
                storage_options=storage_options,
            )

        return AsyncTable(new_table)

    async def open_table(
        self,
        name: str,
        storage_options: Optional[Dict[str, str]] = None,
        index_cache_size: Optional[int] = None,
    ) -> AsyncTable:
        """Open a Lance Table in the database.

        Parameters
        ----------
        name: str
            The name of the table.
        storage_options: dict, optional
            Additional options for the storage backend. Options already set on the
            connection will be inherited by the table, but can be overridden here.
            See available options at
            <https://lancedb.github.io/lancedb/guides/storage/>
        index_cache_size: int, default 256
            **Deprecated**: Use session-level cache configuration instead.
            Create a Session with custom cache sizes and pass it to lancedb.connect().

            Set the size of the index cache, specified as a number of entries

            The exact meaning of an "entry" will depend on the type of index:
            * IVF - there is one entry for each IVF partition
            * BTREE - there is one entry for the entire index

            This cache applies to the entire opened table, across all indices.
            Setting this value higher will increase performance on larger datasets
            at the expense of more RAM

        Returns
        -------
        A LanceTable object representing the table.
        """
        table = await self._inner.open_table(name, storage_options, index_cache_size)
        return AsyncTable(table)

    async def rename_table(self, old_name: str, new_name: str):
        """Rename a table in the database.

        Parameters
        ----------
        old_name: str
            The current name of the table.
        new_name: str
            The new name of the table.
        """
        await self._inner.rename_table(old_name, new_name)

    async def drop_table(self, name: str, *, ignore_missing: bool = False):
        """Drop a table from the database.

        Parameters
        ----------
        name: str
            The name of the table.
        ignore_missing: bool, default False
            If True, ignore if the table does not exist.
        """
        try:
            await self._inner.drop_table(name)
        except ValueError as e:
            if not ignore_missing:
                raise e
            if f"Table '{name}' was not found" not in str(e):
                raise e

    async def drop_all_tables(self):
        """Drop all tables from the database."""
        await self._inner.drop_all_tables()

    @deprecation.deprecated(
        deprecated_in="0.15.1",
        removed_in="0.17",
        current_version=__version__,
        details="Use drop_all_tables() instead",
    )
    async def drop_database(self):
        """
        Drop database
        This is the same thing as dropping all the tables
        """
        await self._inner.drop_all_tables()
