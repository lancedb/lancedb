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

import inspect
import logging
import uuid
from concurrent.futures import ThreadPoolExecutor
from typing import Iterable, List, Optional, Union
from urllib.parse import urlparse

import pyarrow as pa
from overrides import override

from ..common import DATA
from ..db import DBConnection
from ..embeddings import EmbeddingFunctionConfig
from ..pydantic import LanceModel
from ..table import Table, _sanitize_data
from .arrow import to_ipc_binary
from .client import ARROW_STREAM_CONTENT_TYPE, RestfulLanceDBClient
from .errors import LanceDBClientError


class RemoteDBConnection(DBConnection):
    """A connection to a remote LanceDB database."""

    def __init__(
        self,
        db_url: str,
        api_key: str,
        region: str,
        host_override: Optional[str] = None,
        request_thread_pool: Optional[ThreadPoolExecutor] = None,
    ):
        """Connect to a remote LanceDB database."""
        parsed = urlparse(db_url)
        if parsed.scheme != "db":
            raise ValueError(f"Invalid scheme: {parsed.scheme}, only accepts db://")
        self.db_name = parsed.netloc
        self.api_key = api_key
        self._client = RestfulLanceDBClient(
            self.db_name, region, api_key, host_override
        )
        self._request_thread_pool = request_thread_pool

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
        while True:
            result = self._client.list_tables(limit, page_token)

            if len(result) > 0:
                page_token = result[len(result) - 1]
            else:
                break
            for item in result:
                yield item

    @override
    def open_table(self, name: str) -> Table:
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

        self._client.mount_retry_adapter_for_table(name)

        # check if table exists
        try:
            self._client.post(f"/v1/table/{name}/describe/")
        except LanceDBClientError as err:
            if str(err).startswith("Not found"):
                logging.error(
                    "Table %s does not exist. Please first call "
                    "db.create_table(%s, data).",
                    name,
                    name,
                )
        return RemoteTable(self, name)

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
        if data is None and schema is None:
            raise ValueError("Either data or schema must be provided.")
        if embedding_functions is not None:
            logging.warning(
                "embedding_functions is not yet supported on LanceDB Cloud."
                "Please vote https://github.com/lancedb/lancedb/issues/626 "
                "for this feature."
            )
        if mode is not None:
            logging.warning("mode is not yet supported on LanceDB Cloud.")

        if inspect.isclass(schema) and issubclass(schema, LanceModel):
            # convert LanceModel to pyarrow schema
            # note that it's possible this contains
            # embedding function metadata already
            schema = schema.to_arrow_schema()

        if data is not None:
            data = _sanitize_data(
                data,
                schema,
                metadata=None,
                on_bad_vectors=on_bad_vectors,
                fill_value=fill_value,
            )
        else:
            if schema is None:
                raise ValueError("Either data or schema must be provided")
            data = pa.Table.from_pylist([], schema=schema)

        from .table import RemoteTable

        data = to_ipc_binary(data)
        request_id = uuid.uuid4().hex

        self._client.post(
            f"/v1/table/{name}/create/",
            data=data,
            request_id=request_id,
            content_type=ARROW_STREAM_CONTENT_TYPE,
        )

        return RemoteTable(self, name)

    @override
    def drop_table(self, name: str):
        """Drop a table from the database.

        Parameters
        ----------
        name: str
            The name of the table.
        """

        self._client.post(
            f"/v1/table/{name}/drop/",
        )

    async def close(self):
        """Close the connection to the database."""
        self._client.close()
