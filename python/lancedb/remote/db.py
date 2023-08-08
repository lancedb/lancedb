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

import asyncio
import uuid
from typing import List, Optional
from urllib.parse import urlparse

import pyarrow as pa

from lancedb.common import DATA
from lancedb.db import DBConnection
from lancedb.table import Table, _sanitize_data

from .arrow import to_ipc_binary
from .client import ARROW_STREAM_CONTENT_TYPE, RestfulLanceDBClient


class RemoteDBConnection(DBConnection):
    """A connection to a remote LanceDB database."""

    def __init__(
        self,
        db_url: str,
        api_key: str,
        region: str,
        host_override: Optional[str] = None,
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
        try:
            self._loop = asyncio.get_running_loop()
        except RuntimeError:
            self._loop = asyncio.get_event_loop()

    def __repr__(self) -> str:
        return f"RemoveConnect(name={self.db_name})"

    def table_names(self) -> List[str]:
        """List the names of all tables in the database."""
        result = self._loop.run_until_complete(self._client.list_tables())
        return result

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

        # TODO: check if table exists

        return RemoteTable(self, name)

    def create_table(
        self,
        name: str,
        data: DATA = None,
        schema: pa.Schema = None,
        on_bad_vectors: str = "error",
        fill_value: float = 0.0,
    ) -> Table:
        if data is None and schema is None:
            raise ValueError("Either data or schema must be provided.")
        if data is not None:
            data = _sanitize_data(
                data, schema, on_bad_vectors=on_bad_vectors, fill_value=fill_value
            )
        else:
            if schema is None:
                raise ValueError("Either data or schema must be provided")
            data = pa.Table.from_pylist([], schema=schema)

        from .table import RemoteTable

        data = to_ipc_binary(data)
        request_id = uuid.uuid4().hex

        self._loop.run_until_complete(
            self._client.post(
                f"/v1/table/{name}/create/",
                data=data,
                request_id=request_id,
                content_type=ARROW_STREAM_CONTENT_TYPE,
            )
        )
        return RemoteTable(self, name)

    def drop_table(self, name: str):
        """Drop a table from the database.

        Parameters
        ----------
        name: str
            The name of the table.
        """
        self._loop.run_until_complete(
            self._client.post(
                f"/v1/table/{name}/drop/",
            )
        )
