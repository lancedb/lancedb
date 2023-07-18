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
from abc import ABC, abstractmethod
from functools import cached_property
from typing import Iterable, List, Union

import lance
import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.fs
from lance import LanceDataset
from lance.vector import vec_to_table

from .common import DATA, VEC, VECTOR_COLUMN_NAME
from .query import LanceFtsQueryBuilder, LanceQueryBuilder, Query


def _sanitize_data(data, schema, on_bad_vectors, fill_value):
    if isinstance(data, list):
        data = pa.Table.from_pylist(data)
        data = _sanitize_schema(
            data, schema=schema, on_bad_vectors=on_bad_vectors, fill_value=fill_value
        )
    if isinstance(data, dict):
        data = vec_to_table(data)
    if isinstance(data, pd.DataFrame):
        data = pa.Table.from_pandas(data)
        data = _sanitize_schema(
            data, schema=schema, on_bad_vectors=on_bad_vectors, fill_value=fill_value
        )
    if not isinstance(data, (pa.Table, Iterable)):
        raise TypeError(f"Unsupported data type: {type(data)}")
    return data


class Table(ABC):
    """
    A [Table](Table) is a collection of Records in a LanceDB [Database](Database).

    Examples
    --------

    Create using [DBConnection.create_table][lancedb.DBConnection.create_table]
    (more examples in that method's documentation).

    >>> import lancedb
    >>> db = lancedb.connect("./.lancedb")
    >>> table = db.create_table("my_table", data=[{"vector": [1.1, 1.2], "b": 2}])
    >>> table.head()
    pyarrow.Table
    vector: fixed_size_list<item: float>[2]
      child 0, item: float
    b: int64
    ----
    vector: [[[1.1,1.2]]]
    b: [[2]]

    Can append new data with [Table.add()][lancedb.table.Table.add].

    >>> table.add([{"vector": [0.5, 1.3], "b": 4}])

    Can query the table with [Table.search][lancedb.table.Table.search].

    >>> table.search([0.4, 0.4]).select(["b"]).to_df()
       b      vector  score
    0  4  [0.5, 1.3]   0.82
    1  2  [1.1, 1.2]   1.13

    Search queries are much faster when an index is created. See
    [Table.create_index][lancedb.table.Table.create_index].
    """

    @abstractmethod
    def schema(self) -> pa.Schema:
        """Return the [Arrow Schema](https://arrow.apache.org/docs/python/api/datatypes.html#) of
        this [Table](Table)

        """
        raise NotImplementedError

    def to_pandas(self) -> pd.DataFrame:
        """Return the table as a pandas DataFrame.

        Returns
        -------
        pd.DataFrame
        """
        return self.to_arrow().to_pandas()

    @abstractmethod
    def to_arrow(self) -> pa.Table:
        """Return the table as a pyarrow Table.

        Returns
        -------
        pa.Table
        """
        raise NotImplementedError

    def create_index(
        self,
        metric="L2",
        num_partitions=256,
        num_sub_vectors=96,
        vector_column_name: str = VECTOR_COLUMN_NAME,
        replace: bool = True,
    ):
        """Create an index on the table.

        Parameters
        ----------
        metric: str, default "L2"
            The distance metric to use when creating the index.
            Valid values are "L2", "cosine", or "dot".
            L2 is euclidean distance.
        num_partitions: int
            The number of IVF partitions to use when creating the index.
            Default is 256.
        num_sub_vectors: int
            The number of PQ sub-vectors to use when creating the index.
            Default is 96.
        vector_column_name: str, default "vector"
            The vector column name to create the index.
        replace: bool, default True
            If True, replace the existing index if it exists.
            If False, raise an error if duplicate index exists.
        """
        raise NotImplementedError

    @abstractmethod
    def add(
        self,
        data: DATA,
        mode: str = "append",
        on_bad_vectors: str = "error",
        fill_value: float = 0.0,
    ):
        """Add more data to the [Table](Table).

        Parameters
        ----------
        data: list-of-dict, dict, pd.DataFrame
            The data to insert into the table.
        mode: str
            The mode to use when writing the data. Valid values are
            "append" and "overwrite".
        on_bad_vectors: str, default "error"
            What to do if any of the vectors are not the same size or contains NaNs.
            One of "error", "drop", "fill".
        fill_value: float, default 0.
            The value to use when filling vectors. Only used if on_bad_vectors="fill".

        """
        raise NotImplementedError

    @abstractmethod
    def search(
        self, query: Union[VEC, str], vector_column: str = VECTOR_COLUMN_NAME
    ) -> LanceQueryBuilder:
        """Create a search query to find the nearest neighbors
        of the given query vector.

        Parameters
        ----------
        query: list, np.ndarray
            The query vector.
        vector_column: str, default "vector"
            The name of the vector column to search.

        Returns
        -------
        LanceQueryBuilder
            A query builder object representing the query.
            Once executed, the query returns selected columns, the vector,
            and also the "score" column which is the distance between the query
            vector and the returned vector.
        """
        raise NotImplementedError

    @abstractmethod
    def _execute_query(self, query: Query) -> pa.Table:
        pass

    @abstractmethod
    def delete(self, where: str):
        """Delete rows from the table.

        This can be used to delete a single row, many rows, all rows, or
        sometimes no rows (if your predicate matches nothing).

        Parameters
        ----------
        where: str
            The SQL where clause to use when deleting rows. For example, 'x = 2'
            or 'x IN (1, 2, 3)'. The filter must not be empty, or it will error.

        Examples
        --------
        >>> import lancedb
        >>> import pandas as pd
        >>> data = pd.DataFrame({"x": [1, 2, 3], "vector": [[1, 2], [3, 4], [5, 6]]})
        >>> db = lancedb.connect("./.lancedb")
        >>> table = db.create_table("my_table", data)
        >>> table.to_pandas()
           x      vector
        0  1  [1.0, 2.0]
        1  2  [3.0, 4.0]
        2  3  [5.0, 6.0]
        >>> table.delete("x = 2")
        >>> table.to_pandas()
           x      vector
        0  1  [1.0, 2.0]
        1  3  [5.0, 6.0]

        If you have a list of values to delete, you can combine them into a
        stringified list and use the `IN` operator:

        >>> to_remove = [1, 5]
        >>> to_remove = ", ".join([str(v) for v in to_remove])
        >>> to_remove
        '1, 5'
        >>> table.delete(f"x IN ({to_remove})")
        >>> table.to_pandas()
           x      vector
        0  3  [5.0, 6.0]
        """
        raise NotImplementedError


class LanceTable(Table):
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
            if "_dataset" in self.__dict__:
                del self.__dict__["_dataset"]
        except AttributeError:
            pass

    @property
    def schema(self) -> pa.Schema:
        """Return the schema of the table.

        Returns
        -------
        pa.Schema
            A PyArrow schema object."""
        return self._dataset.schema

    def list_versions(self):
        """List all versions of the table"""
        return self._dataset.versions()

    @property
    def version(self) -> int:
        """Get the current version of the table"""
        return self._dataset.version

    def checkout(self, version: int):
        """Checkout a version of the table. This is an in-place operation.

        This allows viewing previous versions of the table.

        Parameters
        ----------
        version : int
            The version to checkout.

        Examples
        --------
        >>> import lancedb
        >>> db = lancedb.connect("./.lancedb")
        >>> table = db.create_table("my_table", [{"vector": [1.1, 0.9], "type": "vector"}])
        >>> table.version
        1
        >>> table.to_pandas()
               vector    type
        0  [1.1, 0.9]  vector
        >>> table.add([{"vector": [0.5, 0.2], "type": "vector"}])
        >>> table.version
        2
        >>> table.checkout(1)
        >>> table.to_pandas()
               vector    type
        0  [1.1, 0.9]  vector
        """
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
        """Return the table as a pandas DataFrame.

        Returns
        -------
        pd.DataFrame
        """
        return self.to_arrow().to_pandas()

    def to_arrow(self) -> pa.Table:
        """Return the table as a pyarrow Table.

        Returns
        -------
        pa.Table"""
        return self._dataset.to_table()

    @property
    def _dataset_uri(self) -> str:
        return os.path.join(self._conn.uri, f"{self.name}.lance")

    def create_index(
        self,
        metric="L2",
        num_partitions=256,
        num_sub_vectors=96,
        vector_column_name=VECTOR_COLUMN_NAME,
        replace: bool = True,
    ):
        """Create an index on the table."""
        self._dataset.create_index(
            column=vector_column_name,
            index_type="IVF_PQ",
            metric=metric,
            num_partitions=num_partitions,
            num_sub_vectors=num_sub_vectors,
            replace=replace,
        )
        self._reset_dataset()

    def create_fts_index(self, field_names: Union[str, List[str]]):
        """Create a full-text search index on the table.

        Warning - this API is highly experimental and is highly likely to change
        in the future.

        Parameters
        ----------
        field_names: str or list of str
            The name(s) of the field to index.
        """
        from .fts import create_index, populate_index

        if isinstance(field_names, str):
            field_names = [field_names]
        index = create_index(self._get_fts_index_path(), field_names)
        populate_index(index, self, field_names)

    def _get_fts_index_path(self):
        return os.path.join(self._dataset_uri, "_indices", "tantivy")

    @cached_property
    def _dataset(self) -> LanceDataset:
        return lance.dataset(self._dataset_uri, version=self._version)

    def to_lance(self) -> LanceDataset:
        """Return the LanceDataset backing this table."""
        return self._dataset

    def add(
        self,
        data: DATA,
        mode: str = "append",
        on_bad_vectors: str = "error",
        fill_value: float = 0.0,
    ):
        """Add data to the table.

        Parameters
        ----------
        data: list-of-dict, dict, pd.DataFrame
            The data to insert into the table.
        mode: str
            The mode to use when writing the data. Valid values are
            "append" and "overwrite".
        on_bad_vectors: str, default "error"
            What to do if any of the vectors are not the same size or contains NaNs.
            One of "error", "drop", "fill".
        fill_value: float, default 0.
            The value to use when filling vectors. Only used if on_bad_vectors="fill".

        Returns
        -------
        int
            The number of vectors in the table.
        """
        # TODO: manage table listing and metadata separately
        data = _sanitize_data(
            data, self.schema, on_bad_vectors=on_bad_vectors, fill_value=fill_value
        )
        lance.write_dataset(data, self._dataset_uri, mode=mode)
        self._reset_dataset()

    def search(
        self, query: Union[VEC, str], vector_column_name=VECTOR_COLUMN_NAME
    ) -> LanceQueryBuilder:
        """Create a search query to find the nearest neighbors
        of the given query vector.

        Parameters
        ----------
        query: list, np.ndarray
            The query vector.
        vector_column_name: str, default "vector"
            The name of the vector column to search.

        Returns
        -------
        LanceQueryBuilder
            A query builder object representing the query.
            Once executed, the query returns selected columns, the vector,
            and also the "score" column which is the distance between the query
            vector and the returned vector.
        """
        if isinstance(query, str):
            # fts
            return LanceFtsQueryBuilder(self, query, vector_column_name)

        if isinstance(query, list):
            query = np.array(query)
        if isinstance(query, np.ndarray):
            query = query.astype(np.float32)
        else:
            raise TypeError(f"Unsupported query type: {type(query)}")
        return LanceQueryBuilder(self, query, vector_column_name)

    @classmethod
    def create(
        cls,
        db,
        name,
        data=None,
        schema=None,
        mode="create",
        on_bad_vectors: str = "error",
        fill_value: float = 0.0,
    ):
        """
        Create a new table.

        Examples
        --------
        >>> import lancedb
        >>> import pandas as pd
        >>> data = pd.DataFrame({"x": [1, 2, 3], "vector": [[1, 2], [3, 4], [5, 6]]})
        >>> db = lancedb.connect("./.lancedb")
        >>> table = db.create_table("my_table", data)
        >>> table.to_pandas()
           x      vector
        0  1  [1.0, 2.0]
        1  2  [3.0, 4.0]
        2  3  [5.0, 6.0]

        Parameters
        ----------
        db: LanceDB
            The LanceDB instance to create the table in.
        name: str
            The name of the table to create.
        data: list-of-dict, dict, pd.DataFrame, default None
            The data to insert into the table.
            At least one of `data` or `schema` must be provided.
        schema: dict, optional
            The schema of the table. If not provided, the schema is inferred from the data.
            At least one of `data` or `schema` must be provided.
        mode: str, default "create"
            The mode to use when writing the data. Valid values are
            "create", "overwrite", and "append".
        on_bad_vectors: str, default "error"
            What to do if any of the vectors are not the same size or contains NaNs.
            One of "error", "drop", "fill".
        fill_value: float, default 0.
            The value to use when filling vectors. Only used if on_bad_vectors="fill".
        """
        tbl = LanceTable(db, name)
        if data is not None:
            data = _sanitize_data(
                data, schema, on_bad_vectors=on_bad_vectors, fill_value=fill_value
            )
        else:
            if schema is None:
                raise ValueError("Either data or schema must be provided")
            data = pa.Table.from_pylist([], schema=schema)
        lance.write_dataset(data, tbl._dataset_uri, schema=schema, mode=mode)
        return LanceTable(db, name)

    @classmethod
    def open(cls, db, name):
        tbl = cls(db, name)
        fs, path = pa.fs.FileSystem.from_uri(tbl._dataset_uri)
        file_info = fs.get_file_info(path)
        if file_info.type != pa.fs.FileType.Directory:
            raise FileNotFoundError(
                f"Table {name} does not exist. Please first call db.create_table({name}, data)"
            )
        return tbl

    def delete(self, where: str):
        self._dataset.delete(where)

    def _execute_query(self, query: Query) -> pa.Table:
        ds = self.to_lance()
        return ds.to_table(
            columns=query.columns,
            filter=query.filter,
            nearest={
                "column": query.vector_column,
                "q": query.vector,
                "k": query.k,
                "metric": query.metric,
                "nprobes": query.nprobes,
                "refine_factor": query.refine_factor,
            },
        )


def _sanitize_schema(
    data: pa.Table,
    schema: pa.Schema = None,
    on_bad_vectors: str = "error",
    fill_value: float = 0.0,
) -> pa.Table:
    """Ensure that the table has the expected schema.

    Parameters
    ----------
    data: pa.Table
        The table to sanitize.
    schema: pa.Schema; optional
        The expected schema. If not provided, this just converts the
        vector column to fixed_size_list(float32) if necessary.
    on_bad_vectors: str, default "error"
        What to do if any of the vectors are not the same size or contains NaNs.
        One of "error", "drop", "fill".
    fill_value: float, default 0.
        The value to use when filling vectors. Only used if on_bad_vectors="fill".
    """
    if schema is not None:
        if data.schema == schema:
            return data
        # cast the columns to the expected types
        data = data.combine_chunks()
        data = _sanitize_vector_column(
            data,
            vector_column_name=VECTOR_COLUMN_NAME,
            on_bad_vectors=on_bad_vectors,
            fill_value=fill_value,
        )
        return pa.Table.from_arrays(
            [data[name] for name in schema.names], schema=schema
        )
    # just check the vector column
    return _sanitize_vector_column(
        data,
        vector_column_name=VECTOR_COLUMN_NAME,
        on_bad_vectors=on_bad_vectors,
        fill_value=fill_value,
    )


def _sanitize_vector_column(
    data: pa.Table,
    vector_column_name: str,
    on_bad_vectors: str = "error",
    fill_value: float = 0.0,
) -> pa.Table:
    """
    Ensure that the vector column exists and has type fixed_size_list(float32)

    Parameters
    ----------
    data: pa.Table
        The table to sanitize.
    vector_column_name: str
        The name of the vector column.
    on_bad_vectors: str, default "error"
        What to do if any of the vectors are not the same size or contains NaNs.
        One of "error", "drop", "fill".
    fill_value: float, default 0.0
        The value to use when filling vectors. Only used if on_bad_vectors="fill".
    """
    if vector_column_name not in data.column_names:
        raise ValueError(f"Missing vector column: {vector_column_name}")
    # ChunkedArray is annoying to work with, so we combine chunks here
    vec_arr = data[vector_column_name].combine_chunks()
    if pa.types.is_list(data[vector_column_name].type):
        # if it's a variable size list array we make sure the dimensions are all the same
        has_jagged_ndims = len(vec_arr.values) % len(data) != 0
        if has_jagged_ndims:
            data = _sanitize_jagged(
                data, fill_value, on_bad_vectors, vec_arr, vector_column_name
            )
            vec_arr = data[vector_column_name].combine_chunks()
    elif not pa.types.is_fixed_size_list(vec_arr.type):
        raise TypeError(f"Unsupported vector column type: {vec_arr.type}")

    vec_arr = ensure_fixed_size_list_of_f32(vec_arr)
    data = data.set_column(
        data.column_names.index(vector_column_name), vector_column_name, vec_arr
    )

    has_nans = pc.any(pc.is_nan(vec_arr.values)).as_py()
    if has_nans:
        data = _sanitize_nans(
            data, fill_value, on_bad_vectors, vec_arr, vector_column_name
        )

    return data


def ensure_fixed_size_list_of_f32(vec_arr):
    values = vec_arr.values
    if not pa.types.is_float32(values.type):
        values = values.cast(pa.float32())
    if pa.types.is_fixed_size_list(vec_arr.type):
        list_size = vec_arr.type.list_size
    else:
        list_size = len(values) / len(vec_arr)
    vec_arr = pa.FixedSizeListArray.from_arrays(values, list_size)
    return vec_arr


def _sanitize_jagged(data, fill_value, on_bad_vectors, vec_arr, vector_column_name):
    """Sanitize jagged vectors."""
    if on_bad_vectors == "error":
        raise ValueError(
            f"Vector column {vector_column_name} has variable length vectors "
            "Set on_bad_vectors='drop' to remove them, or "
            "set on_bad_vectors='fill' and fill_value=<value> to replace them."
        )

    lst_lengths = pc.list_value_length(vec_arr)
    ndims = pc.max(lst_lengths).as_py()
    correct_ndims = pc.equal(lst_lengths, ndims)

    if on_bad_vectors == "fill":
        if fill_value is None:
            raise ValueError(
                "`fill_value` must not be None if `on_bad_vectors` is 'fill'"
            )
        fill_arr = pa.scalar([float(fill_value)] * ndims)
        vec_arr = pc.if_else(correct_ndims, vec_arr, fill_arr)
        data = data.set_column(
            data.column_names.index(vector_column_name), vector_column_name, vec_arr
        )
    elif on_bad_vectors == "drop":
        data = data.filter(correct_ndims)
    return data


def _sanitize_nans(data, fill_value, on_bad_vectors, vec_arr, vector_column_name):
    """Sanitize NaNs in vectors"""
    if on_bad_vectors == "error":
        raise ValueError(
            f"Vector column {vector_column_name} has NaNs. "
            "Set on_bad_vectors='drop' to remove them, or "
            "set on_bad_vectors='fill' and fill_value=<value> to replace them."
        )
    elif on_bad_vectors == "fill":
        if fill_value is None:
            raise ValueError(
                "`fill_value` must not be None if `on_bad_vectors` is 'fill'"
            )
        fill_value = float(fill_value)
        values = pc.if_else(pc.is_nan(vec_arr.values), fill_value, vec_arr.values)
        ndims = len(vec_arr[0])
        vec_arr = pa.FixedSizeListArray.from_arrays(values, ndims)
        data = data.set_column(
            data.column_names.index(vector_column_name), vector_column_name, vec_arr
        )
    elif on_bad_vectors == "drop":
        is_value_nan = pc.is_nan(vec_arr.values).to_numpy(zero_copy_only=False)
        is_full = np.any(~is_value_nan.reshape(-1, vec_arr.type.list_size), axis=1)
        data = data.filter(is_full)
    return data
