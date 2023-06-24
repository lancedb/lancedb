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

import functools
import os
from pathlib import Path

import pyarrow as pa
from pyarrow import fs

from .common import DATA, URI
from .table import LanceTable
from .util import get_uri_location, get_uri_scheme


class LanceDBConnection:
    """
    A connection to a LanceDB database.

    Parameters
    ----------
    uri: str or Path
        The root uri of the database.

    Examples
    --------
    >>> import lancedb
    >>> db = lancedb.connect("./.lancedb")
    >>> db.create_table("my_table", data=[{"vector": [1.1, 1.2], "b": 2},
    ...                                   {"vector": [0.5, 1.3], "b": 4}])
    LanceTable(my_table)
    >>> db.create_table("another_table", data=[{"vector": [0.4, 0.4], "b": 6}])
    LanceTable(another_table)
    >>> sorted(db.table_names())
    ['another_table', 'my_table']
    >>> len(db)
    2
    >>> db["my_table"]
    LanceTable(my_table)
    >>> "my_table" in db
    True
    >>> db.drop_table("my_table")
    >>> db.drop_table("another_table")
    """

    def __init__(self, uri: URI):
        if not isinstance(uri, Path):
            scheme = get_uri_scheme(uri)
        is_local = isinstance(uri, Path) or scheme == "file"
        # managed lancedb remote uses schema like lancedb+[http|grpc|...]://
        self._is_managed_remote = not is_local and scheme.startswith("lancedb")
        if self._is_managed_remote:
            if len(scheme.split("+")) != 2:
                raise ValueError(
                    f"Invalid LanceDB URI: {uri}, expected uri to have scheme like lancedb+<flavor>://..."
                )
        if is_local:
            if isinstance(uri, str):
                uri = Path(uri)
            uri = uri.expanduser().absolute()
            Path(uri).mkdir(parents=True, exist_ok=True)
        self._uri = str(uri)

        self._entered = False

    @property
    def uri(self) -> str:
        return self._uri

    @functools.cached_property
    def is_managed_remote(self) -> bool:
        return self._is_managed_remote

    @functools.cached_property
    def remote_flavor(self) -> str:
        if not self.is_managed_remote:
            raise ValueError(
                "Not a managed remote LanceDB, there should be no server flavor"
            )
        return get_uri_scheme(self.uri).split("+")[1]

    @functools.cached_property
    def _client(self) -> "lancedb.remote.LanceDBClient":
        if not self.is_managed_remote:
            raise ValueError("Not a managed remote LanceDB, there should be no client")

        # don't import unless we are really using remote
        from lancedb.remote.client import RestfulLanceDBClient

        if self.remote_flavor == "http":
            return RestfulLanceDBClient(self._uri)

        raise ValueError("Unsupported remote flavor: " + self.remote_flavor)

    async def close(self):
        if self._entered:
            raise ValueError("Cannot re-enter the same LanceDBConnection twice")
        self._entered = True
        await self._client.close()

    async def __aenter__(self) -> LanceDBConnection:
        return self

    async def __aexit__(self, exc_type, exc_value, traceback):
        await self.close()

    def table_names(self) -> list[str]:
        """Get the names of all tables in the database.

        Returns
        -------
        list of str
            A list of table names.
        """
        try:
            filesystem, path = fs.FileSystem.from_uri(self.uri)
        except pa.ArrowInvalid:
            raise NotImplementedError("Unsupported scheme: " + self.uri)

        try:
            paths = filesystem.get_file_info(
                fs.FileSelector(get_uri_location(self.uri))
            )
        except FileNotFoundError:
            # It is ok if the file does not exist since it will be created
            paths = []
        tables = [
            os.path.splitext(file_info.base_name)[0]
            for file_info in paths
            if file_info.extension == "lance"
        ]
        return tables

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
        LanceTable
            A reference to the newly created table.

        Examples
        --------

        Can create with list of tuples or dictionaries:

        >>> import lancedb
        >>> db = lancedb.connect("./.lancedb")
        >>> data = [{"vector": [1.1, 1.2], "lat": 45.5, "long": -122.7},
        ...         {"vector": [0.2, 1.8], "lat": 40.1, "long":  -74.1}]
        >>> db.create_table("my_table", data)
        LanceTable(my_table)
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
        LanceTable(table2)
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
        convert to or else provide a PyArrow table directly.

        >>> custom_schema = pa.schema([
        ...   pa.field("vector", pa.list_(pa.float32(), 2)),
        ...   pa.field("lat", pa.float32()),
        ...   pa.field("long", pa.float32())
        ... ])
        >>> db.create_table("table3", data, schema = custom_schema)
        LanceTable(table3)
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

    def drop_table(self, name: str):
        """Drop a table from the database.

        Parameters
        ----------
        name: str
            The name of the table.
        """
        filesystem, path = pa.fs.FileSystem.from_uri(self.uri)
        table_path = os.path.join(path, name + ".lance")
        filesystem.delete_dir(table_path)
