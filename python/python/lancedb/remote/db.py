#  Copyright 2023 LanceDB Developers
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

from datetime import timedelta
import logging
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Dict, Iterable, List, Optional, Union
from urllib.parse import urlparse
import warnings

from lancedb import connect_async
from lancedb.remote import ClientConfig
import pyarrow as pa
from overrides import override

from ..common import DATA
from ..db import DBConnection, LOOP
from ..embeddings import EmbeddingFunctionConfig
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
    def table_names(
        self, page_token: Optional[str] = None, limit: int = 10
    ) -> Iterable[str]:
        """List the names of all tables in the database.

        Parameters
        ----------
        page_token: str
            The last token to start the new page.
        limit: int, default 10
            The maximum number of tables to return for each page.

        Returns
        -------
        An iterator of table names.
        """
        return LOOP.run(self._conn.table_names(start_after=page_token, limit=limit))

    @override
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

        Returns
        -------
        A LanceTable object representing the table.
        """
        from .table import RemoteTable

        if index_cache_size is not None:
            logging.info(
                "index_cache_size is ignored in LanceDb Cloud"
                " (there is no local cache to configure)"
            )

        table = LOOP.run(self._conn.open_table(name))
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
        embedding_functions: Optional[List[EmbeddingFunctionConfig]] = None,
    ) -> Table:
        """Create a [Table][lancedb.table.Table] in the database.

        Parameters
        ----------
        name: str
            The name of the table.
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
                mode=mode,
                schema=schema,
                on_bad_vectors=on_bad_vectors,
                fill_value=fill_value,
            )
        )
        return RemoteTable(table, self.db_name)

    @override
    def drop_table(self, name: str):
        """Drop a table from the database.

        Parameters
        ----------
        name: str
            The name of the table.
        """
        LOOP.run(self._conn.drop_table(name))

    @override
    def rename_table(self, cur_name: str, new_name: str):
        """Rename a table in the database.

        Parameters
        ----------
        cur_name: str
            The current name of the table.
        new_name: str
            The new name of the table.
        """
        LOOP.run(self._conn.rename_table(cur_name, new_name))

    async def close(self):
        """Close the connection to the database."""
        self._client.close()
