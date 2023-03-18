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

from functools import cached_property
from pathlib import Path
from typing import Union

import lance
from lance import LanceDataset
from lance.vector import vec_to_table
import numpy as np
import pandas as pd
import pyarrow as pa

VEC = Union[list, np.ndarray, pa.Array, pa.ChunkedArray]
URI = Union[str, Path]

# TODO support generator
DATA = Union[list[dict], dict, pd.DataFrame]
VECTOR_COLUMN_NAME = "vector"


class LanceDBConnection:
    """
    A connection to a LanceDB database.
    """

    def __init__(self, uri: URI):
        if isinstance(uri, str):
            uri = Path(uri)
        uri = uri.expanduser().absolute()
        self.uri = uri

    def create_table(self, name: str, data: DATA = None) -> LanceTable:
        """Create a table in the database.

        Parameters
        ----------
        name: str
            The name of the table.
        data: list, tuple, dict, pd.DataFrame; optional
            The data to insert into the table.

        Returns
        -------
        A LanceTable object representing the table.
        """
        tbl = LanceTable(self, name)
        if data is not None:
            tbl.add(data)
        return tbl


class LanceTable:
    """
    A table in a LanceDB database.
    """

    def __init__(self, connection: LanceDBConnection, name: str, schema: pa.Schema = None):
        self._conn = connection
        self.name = name
        self.schema = schema

    @property
    def _dataset_uri(self) -> str:
        return str(self._conn.uri / f"{self.name}.lance")

    @cached_property
    def _dataset(self) -> LanceDataset:
        return lance.dataset(self._dataset_uri)

    def add(self, data: DATA) -> int:
        """Add data to the table.

        Parameters
        ----------
        data: list-of-dict, dict, pd.DataFrame
            The data to insert into the table.

        Returns
        -------
        The number of vectors added to the table.
        """
        if isinstance(data, list):
            data = pa.Table.from_pylist(data)
            data = _sanitize_schema(data, schema=self.schema)
        if isinstance(data, dict):
            data = vec_to_table(data)
        if isinstance(data, pd.DataFrame):
            data = pa.Table.from_pandas(data)
            data = _sanitize_schema(data, schema=self.schema)
        if not isinstance(data, pa.Table):
            raise TypeError(f"Unsupported data type: {type(data)}")
        ds = lance.write_dataset(data, self._dataset_uri, mode="append")
        return ds.count_rows()

    def search(self, query: VEC) -> LanceQueryBuilder:
        """Create a search query to find the nearest neighbors
        of the given query vector.

        Parameters
        ----------
        query: list, np.ndarray
            The query vector.

        Returns
        -------
        A LanceQueryBuilder object representing the query.
        """
        if isinstance(query, list):
            query = np.array(query)
        if isinstance(query, np.ndarray):
            query = query.astype(np.float32)
        else:
            raise TypeError(f"Unsupported query type: {type(query)}")
        return LanceQueryBuilder(self, query)


def _sanitize_schema(data: pa.Table, schema: pa.Schema = None) -> pa.Table:
    """Ensure that the table has the expected schema.

    Parameters
    ----------
    data: pa.Table
        The table to sanitize.
    schema: pa.Schema; optional
        The expected schema. If not provided, this just converts the
        vector column to fixed_size_list(float32) if necessary.
    """
    if schema is not None:
        if data.schema == schema:
            return data
        # cast the columns to the expected types
        data = data.combine_chunks()
        return pa.Table.from_arrays([
            data[name].cast(schema.field(name).type)
            for name in schema.names
        ], schema=schema)
    # just check the vector column
    return _sanitize_vector_column(data, vector_column_name=VECTOR_COLUMN_NAME)


def _sanitize_vector_column(data: pa.Table, vector_column_name: str) -> pa.Table:
    """
    Ensure that the vector column exists and has type fixed_size_list(float32)

    Parameters
    ----------
    data: pa.Table
        The table to sanitize.
    vector_column_name: str
        The name of the vector column.
    """
    i = data.column_names.index(vector_column_name)
    if i < 0:
        raise ValueError(f"Missing vector column: {vector_column_name}")
    vec_arr = data[vector_column_name].combine_chunks()
    if pa.types.is_fixed_size_list(vec_arr.type):
        return data
    if not pa.types.is_list(vec_arr.type):
        raise TypeError(f"Unsupported vector column type: {vec_arr.type}")
    values = vec_arr.values
    if not pa.types.is_float32(values.type):
        values = values.cast(pa.float32())
    list_size = len(values) / len(data)
    vec_arr = pa.FixedSizeListArray.from_arrays(values, list_size)
    return data.set_column(i, vector_column_name, vec_arr)


class LanceQueryBuilder:
    """
    A builder for nearest neighbor queries for LanceDB.
    """

    def __init__(self, table: LanceTable, query: np.ndarray):
        self._table = table
        self._query = query
        self._limit = 10
        self._columns = None
        self._where = None

    def limit(self, limit: int) -> LanceQueryBuilder:
        """Set the maximum number of results to return.

        Parameters
        ----------
        limit: int
            The maximum number of results to return.

        Returns
        -------
        The LanceQueryBuilder object.
        """
        self._limit = limit
        return self

    def select(self, columns: list) -> LanceQueryBuilder:
        """Set the columns to return.

        Parameters
        ----------
        columns: list
            The columns to return.

        Returns
        -------
        The LanceQueryBuilder object.
        """
        self._columns = columns
        return self

    def where(self, where: str) -> LanceQueryBuilder:
        """Set the where clause.

        Parameters
        ----------
        where: str
            The where clause.

        Returns
        -------
        The LanceQueryBuilder object.
        """
        self._where = where
        return self

    def to_df(self) -> pd.DataFrame:
        """Execute the query and return the results as a pandas DataFrame.
        """
        ds = self._table._dataset
        # TODO indexed search
        import pdb; pdb.set_trace()
        tbl = ds.to_table(
            columns=self._columns,
            filter=self._where,
            nearest={
                "column": VECTOR_COLUMN_NAME,
                "q": self._query,
                "k": self._limit
            }
        )
        return tbl.to_pandas()


