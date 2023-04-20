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

from __future__ import annotations

from pathlib import Path
import pyarrow as pa

from .common import URI, DATA
from .table import LanceTable


class LanceDBConnection:
    """
    A connection to a LanceDB database.
    """

    def __init__(self, uri: URI):
        if isinstance(uri, str):
            uri = Path(uri)
        uri = uri.expanduser().absolute()
        Path(uri).mkdir(parents=True, exist_ok=True)
        self._uri = str(uri)

    @property
    def uri(self) -> str:
        return self._uri

    def table_names(self) -> list[str]:
        """Get the names of all tables in the database.

        Returns
        -------
        A list of table names.
        """
        return [p.stem for p in Path(self.uri).glob("*.lance")]

    def __len__(self) -> int:
        return len(self.table_names())

    def __contains__(self, name: str) -> bool:
        return name in self.table_names()

    def __getitem__(self, name: str) -> LanceTable:
        return self.open_table(name)

    def create_table(
        self,
        name: str,
        data: DATA = None,
        schema: pa.Schema = None,
        mode: str = "create",
    ) -> LanceTable:
        """Create a table in the database.

        Parameters
        ----------
        name: str
            The name of the table.
        data: list, tuple, dict, pd.DataFrame; optional
            The data to insert into the table.
        schema: pyarrow.Schema; optional
            The schema of the table.
        mode: str; default "create"
            The mode to use when creating the table.
            By default, if the table already exists, an exception is raised.
            If you want to overwrite the table, use mode="overwrite".

        Note
        ----
        The vector index won't be created by default.
        To create the index, call the `create_index` method on the table.

        Returns
        -------
        A LanceTable object representing the table.
        """
        if data is not None:
            tbl = LanceTable.create(self, name, data, schema, mode=mode)
        else:
            tbl = LanceTable(self, name)
        return tbl

    def open_table(self, name: str) -> LanceTable:
        """Open a table in the database.

        Parameters
        ----------
        name: str
            The name of the table.

        Returns
        -------
        A LanceTable object representing the table.
        """
        return LanceTable(self, name)
