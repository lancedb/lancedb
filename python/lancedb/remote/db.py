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
import inspect
import uuid
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

    @override
    def table_names(self, page_token: Optional[str] = None, limit=10) -> Iterable[str]:
        """List the names of all tables in the database.

        Parameters
        ----------
        last_token: str
            The last token to start the new page.

        Returns
        -------
        An iterator of table names.
        """
        while True:
            result = self._loop.run_until_complete(
                self._client.list_tables(limit, page_token)
            )
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

        # TODO: check if table exists

        return RemoteTable(self, name)

    @override
    def create_table(
        self,
        name: str,
        data: DATA = None,
        schema: Optional[Union[pa.Schema, LanceModel]] = None,
        on_bad_vectors: str = "error",
        fill_value: float = 0.0,
        embedding_functions: Optional[List[EmbeddingFunctionConfig]] = None,
    ) -> Table:
        if data is None and schema is None:
            raise ValueError("Either data or schema must be provided.")
        if embedding_functions is not None:
            raise NotImplementedError(
                "embedding_functions is not supported for remote databases."
                "Please vote https://github.com/lancedb/lancedb/issues/626 "
                "for this feature."
            )

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

        self._loop.run_until_complete(
            self._client.post(
                f"/v1/table/{name}/create/",
                data=data,
                request_id=request_id,
                content_type=ARROW_STREAM_CONTENT_TYPE,
            )
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
        self._loop.run_until_complete(
            self._client.post(
                f"/v1/table/{name}/drop/",
            )
        )

    async def close(self):
        """Close the connection to the database."""
        self._loop.close()
        await self._client.close()
