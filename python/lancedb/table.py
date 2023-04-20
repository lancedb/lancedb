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

import os
from functools import cached_property

import lance
import numpy as np
import pandas as pd
from lance import LanceDataset
import pyarrow as pa
from lance.vector import vec_to_table

from .query import LanceQueryBuilder
from .common import DATA, VECTOR_COLUMN_NAME, VEC


def _sanitize_data(data, schema):
    if isinstance(data, list):
        data = pa.Table.from_pylist(data)
        data = _sanitize_schema(data, schema=schema)
    if isinstance(data, dict):
        data = vec_to_table(data)
    if isinstance(data, pd.DataFrame):
        data = pa.Table.from_pandas(data)
        data = _sanitize_schema(data, schema=schema)
    if not isinstance(data, pa.Table):
        raise TypeError(f"Unsupported data type: {type(data)}")
    return data


class LanceTable:
    """
    A table in a LanceDB database.
    """

    def __init__(
        self, connection: "lancedb.db.LanceDBConnection", name: str, version: int = None
    ):
        self._conn = connection
        self.name = name
        self._version = version

    def _reset_dataset(self):
        try:
            del self.__dict__["_dataset"]
        except AttributeError:
            pass

    @property
    def schema(self) -> pa.Schema:
        """Return the schema of the table."""
        return self._dataset.schema

    def list_versions(self):
        """List all versions of the table"""
        return self._dataset.versions()

    @property
    def version(self):
        """Get the current version of the table"""
        return self._dataset.version

    def checkout(self, version: int):
        """Checkout a version of the table"""
        max_ver = max([v["version"] for v in self._dataset.versions()])
        if version < 1 or version > max_ver:
            raise ValueError(f"Invalid version {version}")
        self._version = version
        self._reset_dataset()

    def __len__(self):
        return self._dataset.count_rows()

    def __repr__(self) -> str:
        return f"LanceTable({self.name})"

    def __str__(self) -> str:
        return self.__repr__()

    def head(self, n=5) -> pa.Table:
        """Return the first n rows of the table."""
        return self._dataset.head(n)

    def to_pandas(self) -> pd.DataFrame:
        """Return the table as a pandas DataFrame."""
        return self.to_arrow().to_pandas()

    def to_arrow(self) -> pa.Table:
        """Return the table as a pyarrow Table."""
        return self._dataset.to_table()

    @property
    def _dataset_uri(self) -> str:
        return os.path.join(self._conn.uri, f"{self.name}.lance")

    def create_index(self, num_partitions=256, num_sub_vectors=96):
        """Create an index on the table.

        Parameters
        ----------
        num_partitions: int
            The number of IVF partitions to use when creating the index.
            Default is 256.
        num_sub_vectors: int
            The number of PQ sub-vectors to use when creating the index.
            Default is 96.
        """
        self._dataset.create_index(
            column=VECTOR_COLUMN_NAME,
            index_type="IVF_PQ",
            num_partitions=num_partitions,
            num_sub_vectors=num_sub_vectors,
        )
        self._reset_dataset()

    @cached_property
    def _dataset(self) -> LanceDataset:
        return lance.dataset(self._dataset_uri, version=self._version)

    def to_lance(self) -> LanceDataset:
        """Return the LanceDataset backing this table."""
        return self._dataset

    def add(self, data: DATA, mode: str = "append") -> int:
        """Add data to the table.

        Parameters
        ----------
        data: list-of-dict, dict, pd.DataFrame
            The data to insert into the table.
        mode: str
            The mode to use when writing the data. Valid values are
            "append" and "overwrite".

        Returns
        -------
        The number of vectors added to the table.
        """
        data = _sanitize_data(data, self.schema)
        lance.write_dataset(data, self._dataset_uri, mode=mode)
        self._reset_dataset()
        return len(self)

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

    @classmethod
    def create(cls, db, name, data, schema=None, mode="create"):
        tbl = LanceTable(db, name)
        data = _sanitize_data(data, schema)
        lance.write_dataset(data, tbl._dataset_uri, mode=mode)
        return tbl


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
        data = _sanitize_vector_column(data, vector_column_name=VECTOR_COLUMN_NAME)
        return pa.Table.from_arrays(
            [data[name] for name in schema.names], schema=schema
        )
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
