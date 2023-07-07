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

from typing import List
from urllib.parse import urlparse

import pyarrow as pa

from lancedb.common import DATA
from lancedb.db import DBConnection
from lancedb.table import Table

from .client import RestfulLanceDBClient


class RemoteDBConnection(DBConnection):
    """A connection to a remote LanceDB database."""

    def __init__(self, db_url: str, api_key: str, region: str):
        """Connect to a remote LanceDB database."""
        parsed = urlparse(db_url)
        if parsed.scheme != "db":
            raise ValueError(f"Invalid scheme: {parsed.scheme}, only accepts db://")
        self.db_name = parsed.netloc
        self.api_key = api_key
        self._client = RestfulLanceDBClient(self.db_name, region, api_key)

    def __repr__(self) -> str:
        return f"RemoveConnect(name={self.db_name})"

    def table_names(self) -> List[str]:
        raise NotImplementedError

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
        mode: str = "create",
        on_bad_vectors: str = "error",
        fill_value: float = 0.0,
    ) -> Table:
        raise NotImplementedError
