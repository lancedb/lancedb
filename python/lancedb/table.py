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

import inspect
import os
from abc import ABC, abstractmethod
from functools import cached_property
from typing import TYPE_CHECKING, Any, Dict, Iterable, List, Optional, Union

import lance
import numpy as np
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.fs as pa_fs
from lance import LanceDataset
from lance.vector import vec_to_table

from .common import DATA, VEC, VECTOR_COLUMN_NAME
from .embeddings import EmbeddingFunctionConfig, EmbeddingFunctionRegistry
from .pydantic import LanceModel, model_to_dict
from .query import LanceQueryBuilder, Query
from .util import (
    fs_from_uri,
    safe_import_pandas,
    safe_import_polars,
    value_to_sql,
    join_uri,
)
from .utils.events import register_event

if TYPE_CHECKING:
    from datetime import timedelta

    from lance.dataset import CleanupStats, ReaderLike


pd = safe_import_pandas()
pl = safe_import_polars()


def _sanitize_data(
    data,
    schema: Optional[pa.Schema],
    metadata: Optional[dict],
    on_bad_vectors: str,
    fill_value: Any,
):
    if isinstance(data, list):
        # convert to list of dict if data is a bunch of LanceModels
        if isinstance(data[0], LanceModel):
            schema = data[0].__class__.to_arrow_schema()
            data = [model_to_dict(d) for d in data]
            data = pa.Table.from_pylist(data, schema=schema)
        else:
            data = pa.Table.from_pylist(data)
    elif isinstance(data, dict):
        data = vec_to_table(data)
    elif pd is not None and isinstance(data, pd.DataFrame):
        data = pa.Table.from_pandas(data, preserve_index=False)
        # Do not serialize Pandas metadata
        meta = data.schema.metadata if data.schema.metadata is not None else {}
        meta = {k: v for k, v in meta.items() if k != b"pandas"}
        data = data.replace_schema_metadata(meta)
    elif pl is not None and isinstance(data, pl.DataFrame):
        data = data.to_arrow()

    if isinstance(data, pa.Table):
        if metadata:
            data = _append_vector_col(data, metadata, schema)
            metadata.update(data.schema.metadata or {})
            data = data.replace_schema_metadata(metadata)
        data = _sanitize_schema(
            data, schema=schema, on_bad_vectors=on_bad_vectors, fill_value=fill_value
        )
    elif isinstance(data, Iterable):
        data = _to_record_batch_generator(
            data, schema, metadata, on_bad_vectors, fill_value
        )
    else:
        raise TypeError(f"Unsupported data type: {type(data)}")
    return data


def _append_vector_col(data: pa.Table, metadata: dict, schema: Optional[pa.Schema]):
    """
    Use the embedding function to automatically embed the source column and add the
    vector column to the table.
    """
    functions = EmbeddingFunctionRegistry.get_instance().parse_functions(metadata)
    for vector_column, conf in functions.items():
        func = conf.function
        if vector_column not in data.column_names:
            col_data = func.compute_source_embeddings_with_retry(
                data[conf.source_column]
            )
            if schema is not None:
                dtype = schema.field(vector_column).type
            else:
                dtype = pa.list_(pa.float32(), len(col_data[0]))
            data = data.append_column(
                pa.field(vector_column, type=dtype), pa.array(col_data, type=dtype)
            )
    return data


def _to_record_batch_generator(
    data: Iterable, schema, metadata, on_bad_vectors, fill_value
):
    for batch in data:
        if not isinstance(batch, pa.RecordBatch):
            table = _sanitize_data(batch, schema, metadata, on_bad_vectors, fill_value)
            for batch in table.to_batches():
                yield batch
        else:
            yield batch


class Table(ABC):
    """
    A Table is a collection of Records in a LanceDB Database.

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

    >>> table.search([0.4, 0.4]).select(["b"]).to_pandas()
       b      vector  _distance
    0  4  [0.5, 1.3]       0.82
    1  2  [1.1, 1.2]       1.13

    Search queries are much faster when an index is created. See
    [Table.create_index][lancedb.table.Table.create_index].
    """

    @property
    @abstractmethod
    def schema(self) -> pa.Schema:
        """The [Arrow Schema](https://arrow.apache.org/docs/python/api/datatypes.html#)
        of this Table

        """
        raise NotImplementedError

    def to_pandas(self) -> "pd.DataFrame":
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
        accelerator: Optional[str] = None,
        index_cache_size: Optional[int] = None,
    ):
        """Create an index on the table.

        Parameters
        ----------
        metric: str, default "L2"
            The distance metric to use when creating the index.
            Valid values are "L2", "cosine", or "dot".
            L2 is euclidean distance.
        num_partitions: int, default 256
            The number of IVF partitions to use when creating the index.
            Default is 256.
        num_sub_vectors: int, default 96
            The number of PQ sub-vectors to use when creating the index.
            Default is 96.
        vector_column_name: str, default "vector"
            The vector column name to create the index.
        replace: bool, default True
            - If True, replace the existing index if it exists.

            - If False, raise an error if duplicate index exists.
        accelerator: str, default None
            If set, use the given accelerator to create the index.
            Only support "cuda" for now.
        index_cache_size : int, optional
            The size of the index cache in number of entries. Default value is 256.
        """
        raise NotImplementedError

    @abstractmethod
    def create_scalar_index(
        self,
        column: str,
        *,
        replace: bool = True,
    ):
        """Create a scalar index on a column.

        Scalar indices, like vector indices, can be used to speed up scans.  A scalar
        index can speed up scans that contain filter expressions on the indexed column.
        For example, the following scan will be faster if the column ``my_col`` has
        a scalar index:

        .. code-block:: python

            import lancedb

            db = lancedb.connect("/data/lance")
            img_table = db.open_table("images")
            my_df = img_table.search().where("my_col = 7", prefilter=True).to_pandas()

        Scalar indices can also speed up scans containing a vector search and a
        prefilter:

        .. code-block::python

            import lancedb

            db = lancedb.connect("/data/lance")
            img_table = db.open_table("images")
            img_table.search([1, 2, 3, 4], vector_column_name="vector")
                .where("my_col != 7", prefilter=True)
                .to_pandas()

        Scalar indices can only speed up scans for basic filters using
        equality, comparison, range (e.g. ``my_col BETWEEN 0 AND 100``), and set
        membership (e.g. `my_col IN (0, 1, 2)`)

        Scalar indices can be used if the filter contains multiple indexed columns and
        the filter criteria are AND'd or OR'd together
        (e.g. ``my_col < 0 AND other_col> 100``)

        Scalar indices may be used if the filter contains non-indexed columns but,
        depending on the structure of the filter, they may not be usable.  For example,
        if the column ``not_indexed`` does not have a scalar index then the filter
        ``my_col = 0 OR not_indexed = 1`` will not be able to use any scalar index on
        ``my_col``.

        **Experimental API**

        Parameters
        ----------
        column : str
            The column to be indexed.  Must be a boolean, integer, float,
            or string column.
        replace : bool, default True
            Replace the existing index if it exists.

        Examples
        --------

        .. code-block:: python

            import lance

            dataset = lance.dataset("/tmp/images.lance")
            dataset.create_scalar_index("category")
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
        data: DATA
            The data to insert into the table. Acceptable types are:

            - dict or list-of-dict

            - pandas.DataFrame

            - pyarrow.Table or pyarrow.RecordBatch
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
        self,
        query: Optional[Union[VEC, str, "PIL.Image.Image"]] = None,
        vector_column_name: str = VECTOR_COLUMN_NAME,
        query_type: str = "auto",
    ) -> LanceQueryBuilder:
        """Create a search query to find the nearest neighbors
        of the given query vector. We currently support [vector search][search]
        and [full-text search][experimental-full-text-search].

        All query options are defined in [Query][lancedb.query.Query].

        Examples
        --------
        >>> import lancedb
        >>> db = lancedb.connect("./.lancedb")
        >>> data = [
        ...    {"original_width": 100, "caption": "bar", "vector": [0.1, 2.3, 4.5]},
        ...    {"original_width": 2000, "caption": "foo",  "vector": [0.5, 3.4, 1.3]},
        ...    {"original_width": 3000, "caption": "test", "vector": [0.3, 6.2, 2.6]}
        ... ]
        >>> table = db.create_table("my_table", data)
        >>> query = [0.4, 1.4, 2.4]
        >>> (table.search(query, vector_column_name="vector")
        ...     .where("original_width > 1000", prefilter=True)
        ...     .select(["caption", "original_width"])
        ...     .limit(2)
        ...     .to_pandas())
          caption  original_width           vector  _distance
        0     foo            2000  [0.5, 3.4, 1.3]   5.220000
        1    test            3000  [0.3, 6.2, 2.6]  23.089996

        Parameters
        ----------
        query: list/np.ndarray/str/PIL.Image.Image, default None
            The targetted vector to search for.

            - *default None*.
            Acceptable types are: list, np.ndarray, PIL.Image.Image

            - If None then the select/where/limit clauses are applied to filter
            the table
        vector_column_name: str
            The name of the vector column to search.
            *default "vector"*
        query_type: str
            *default "auto"*.
            Acceptable types are: "vector", "fts", or "auto"

            - If "auto" then the query type is inferred from the query;

                - If `query` is a list/np.ndarray then the query type is
                "vector";

                - If `query` is a PIL.Image.Image then either do vector search,
                or raise an error if no corresponding embedding function is found.

            - If `query` is a string, then the query type is "vector" if the
            table has embedding functions else the query type is "fts"

        Returns
        -------
        LanceQueryBuilder
            A query builder object representing the query.
            Once executed, the query returns

            - selected columns

            - the vector

            - and also the "_distance" column which is the distance between the query
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
            The SQL where clause to use when deleting rows.

            - For example, 'x = 2' or 'x IN (1, 2, 3)'.

            The filter must not be empty, or it will error.

        Examples
        --------
        >>> import lancedb
        >>> data = [
        ...    {"x": 1, "vector": [1, 2]},
        ...    {"x": 2, "vector": [3, 4]},
        ...    {"x": 3, "vector": [5, 6]}
        ... ]
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

    @abstractmethod
    def update(
        self,
        where: Optional[str] = None,
        values: Optional[dict] = None,
        *,
        values_sql: Optional[Dict[str, str]] = None,
    ):
        """
        This can be used to update zero to all rows depending on how many
        rows match the where clause. If no where clause is provided, then
        all rows will be updated.

        Either `values` or `values_sql` must be provided. You cannot provide
        both.

        Parameters
        ----------
        where: str, optional
            The SQL where clause to use when updating rows. For example, 'x = 2'
            or 'x IN (1, 2, 3)'. The filter must not be empty, or it will error.
        values: dict, optional
            The values to update. The keys are the column names and the values
            are the values to set.
        values_sql: dict, optional
            The values to update, expressed as SQL expression strings. These can
            reference existing columns. For example, {"x": "x + 1"} will increment
            the x column by 1.

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
        >>> table.update(where="x = 2", values={"vector": [10, 10]})
        >>> table.to_pandas()
           x        vector
        0  1    [1.0, 2.0]
        1  3    [5.0, 6.0]
        2  2  [10.0, 10.0]
        >>> table.update(values_sql={"x": "x + 1"})
        >>> table.to_pandas()
           x        vector
        0  2    [1.0, 2.0]
        1  4    [5.0, 6.0]
        2  3  [10.0, 10.0]
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

    def _reset_dataset(self, version=None):
        try:
            if "_dataset" in self.__dict__:
                del self.__dict__["_dataset"]
            self._version = version
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

        This allows viewing previous versions of the table. If you wish to
        keep writing to the dataset starting from an old version, then use
        the `restore` function.

        Parameters
        ----------
        version : int
            The version to checkout.

        Examples
        --------
        >>> import lancedb
        >>> db = lancedb.connect("./.lancedb")
        >>> table = db.create_table("my_table",
        ...    [{"vector": [1.1, 0.9], "type": "vector"}])
        >>> table.version
        2
        >>> table.to_pandas()
               vector    type
        0  [1.1, 0.9]  vector
        >>> table.add([{"vector": [0.5, 0.2], "type": "vector"}])
        >>> table.version
        3
        >>> table.checkout(2)
        >>> table.to_pandas()
               vector    type
        0  [1.1, 0.9]  vector
        """
        max_ver = max([v["version"] for v in self._dataset.versions()])
        if version < 1 or version > max_ver:
            raise ValueError(f"Invalid version {version}")
        self._reset_dataset(version=version)

        try:
            # Accessing the property updates the cached value
            _ = self._dataset
        except Exception as e:
            if "not found" in str(e):
                raise ValueError(
                    f"Version {version} no longer exists. Was it cleaned up?"
                )
            else:
                raise e

    def restore(self, version: int = None):
        """Restore a version of the table. This is an in-place operation.

        This creates a new version where the data is equivalent to the
        specified previous version. Data is not copied (as of python-v0.2.1).

        Parameters
        ----------
        version : int, default None
            The version to restore. If unspecified then restores the currently
            checked out version. If the currently checked out version is the
            latest version then this is a no-op.

        Examples
        --------
        >>> import lancedb
        >>> db = lancedb.connect("./.lancedb")
        >>> table = db.create_table("my_table", [
        ...     {"vector": [1.1, 0.9], "type": "vector"}])
        >>> table.version
        2
        >>> table.to_pandas()
               vector    type
        0  [1.1, 0.9]  vector
        >>> table.add([{"vector": [0.5, 0.2], "type": "vector"}])
        >>> table.version
        3
        >>> table.restore(2)
        >>> table.to_pandas()
               vector    type
        0  [1.1, 0.9]  vector
        >>> len(table.list_versions())
        4
        """
        max_ver = max([v["version"] for v in self._dataset.versions()])
        if version is None:
            version = self.version
        elif version < 1 or version > max_ver:
            raise ValueError(f"Invalid version {version}")
        else:
            self.checkout(version)

        if version == max_ver:
            # no-op if restoring the latest version
            return

        self._dataset.restore()
        self._reset_dataset()

    def count_rows(self, filter: Optional[str] = None) -> int:
        """
        Count the number of rows in the table.

        Parameters
        ----------
        filter: str, optional
            A SQL where clause to filter the rows to count.
        """
        return self._dataset.count_rows(filter)

    def __len__(self):
        return self.count_rows()

    def __repr__(self) -> str:
        return f"LanceTable({self.name})"

    def __str__(self) -> str:
        return self.__repr__()

    def head(self, n=5) -> pa.Table:
        """Return the first n rows of the table."""
        return self._dataset.head(n)

    def to_pandas(self) -> "pd.DataFrame":
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

    def to_polars(self, batch_size=None) -> "pl.LazyFrame":
        """Return the table as a polars LazyFrame.

        Parameters
        ----------
        batch_size: int, optional
            Passed to polars. This is the maximum row count for
            scanned pyarrow record batches

        Note
        ----
        1. This requires polars to be installed separately
        2. Currently we've disabled push-down of the filters from polars
           because polars pushdown into pyarrow uses pyarrow compute
           expressions rather than SQl strings (which LanceDB supports)

        Returns
        -------
        pl.LazyFrame
        """
        return pl.scan_pyarrow_dataset(
            self.to_lance(), allow_pyarrow_filter=False, batch_size=batch_size
        )

    @property
    def _dataset_uri(self) -> str:
        return join_uri(self._conn.uri, f"{self.name}.lance")

    def create_index(
        self,
        metric="L2",
        num_partitions=256,
        num_sub_vectors=96,
        vector_column_name=VECTOR_COLUMN_NAME,
        replace: bool = True,
        accelerator: Optional[str] = None,
        index_cache_size: Optional[int] = None,
    ):
        """Create an index on the table."""
        self._dataset.create_index(
            column=vector_column_name,
            index_type="IVF_PQ",
            metric=metric,
            num_partitions=num_partitions,
            num_sub_vectors=num_sub_vectors,
            replace=replace,
            accelerator=accelerator,
            index_cache_size=index_cache_size,
        )
        self._reset_dataset()
        register_event("create_index")

    def create_scalar_index(self, column: str, *, replace: bool = True):
        self._dataset.create_scalar_index(column, index_type="BTREE", replace=replace)

    def create_fts_index(
        self,
        field_names: Union[str, List[str]],
        *,
        replace: bool = False,
        writer_heap_size: Optional[int] = 1024 * 1024 * 1024,
    ):
        """Create a full-text search index on the table.

        Warning - this API is highly experimental and is highly likely to change
        in the future.

        Parameters
        ----------
        field_names: str or list of str
            The name(s) of the field to index.
        replace: bool, default False
            If True, replace the existing index if it exists. Note that this is
            not yet an atomic operation; the index will be temporarily
            unavailable while the new index is being created.
        writer_heap_size: int, default 1GB
        """
        from .fts import create_index, populate_index

        if isinstance(field_names, str):
            field_names = [field_names]

        fs, path = fs_from_uri(self._get_fts_index_path())
        index_exists = fs.get_file_info(path).type != pa_fs.FileType.NotFound
        if index_exists:
            if not replace:
                raise ValueError(
                    f"Index already exists. Use replace=True to overwrite."
                )
            fs.delete_dir(path)

        index = create_index(self._get_fts_index_path(), field_names)
        populate_index(index, self, field_names, writer_heap_size=writer_heap_size)
        register_event("create_fts_index")

    def _get_fts_index_path(self):
        return join_uri(self._dataset_uri, "_indices", "tantivy")

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
        If vector columns are missing and the table
        has embedding functions, then the vector columns
        are automatically computed and added.

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
            data,
            self.schema,
            metadata=self.schema.metadata,
            on_bad_vectors=on_bad_vectors,
            fill_value=fill_value,
        )
        lance.write_dataset(data, self._dataset_uri, schema=self.schema, mode=mode)
        self._reset_dataset()
        register_event("add")

    def merge(
        self,
        other_table: Union[LanceTable, ReaderLike],
        left_on: str,
        right_on: Optional[str] = None,
        schema: Optional[Union[pa.Schema, LanceModel]] = None,
    ):
        """Merge another table into this table.

        Performs a left join, where the dataset is the left side and other_table
        is the right side. Rows existing in the dataset but not on the left will
        be filled with null values, unless Lance doesn't support null values for
        some types, in which case an error will be raised. The only overlapping
        column allowed is the join column. If other overlapping columns exist,
        an error will be raised.

        Parameters
        ----------
        other_table: LanceTable or Reader-like
            The data to be merged. Acceptable types are:
            - Pandas DataFrame, Pyarrow Table, Dataset, Scanner,
            Iterator[RecordBatch], or RecordBatchReader
            - LanceTable
        left_on: str
            The name of the column in the dataset to join on.
        right_on: str or None
            The name of the column in other_table to join on. If None, defaults to
            left_on.
        schema: pa.Schema or LanceModel, optional
            The schema of the other_table.
            If not provided, the schema is inferred from the data.

        Examples
        --------
        >>> import lancedb
        >>> import pyarrow as pa
        >>> df = pa.table({'x': [1, 2, 3], 'y': ['a', 'b', 'c']})
        >>> db = lancedb.connect("./.lancedb")
        >>> table = db.create_table("dataset", df)
        >>> table.to_pandas()
           x  y
        0  1  a
        1  2  b
        2  3  c
        >>> new_df = pa.table({'x': [1, 2, 3], 'z': ['d', 'e', 'f']})
        >>> table.merge(new_df, 'x')
        >>> table.to_pandas()
           x  y  z
        0  1  a  d
        1  2  b  e
        2  3  c  f
        """
        if isinstance(schema, LanceModel):
            schema = schema.to_arrow_schema()
        if isinstance(other_table, LanceTable):
            other_table = other_table.to_lance()
        if isinstance(other_table, LanceDataset):
            other_table = other_table.to_table()
        self._dataset.merge(
            other_table, left_on=left_on, right_on=right_on, schema=schema
        )
        self._reset_dataset()
        register_event("merge")

    @cached_property
    def embedding_functions(self) -> dict:
        """
        Get the embedding functions for the table

        Returns
        -------
        funcs: dict
            A mapping of the vector column to the embedding function
            or empty dict if not configured.
        """
        return EmbeddingFunctionRegistry.get_instance().parse_functions(
            self.schema.metadata
        )

    def search(
        self,
        query: Optional[Union[VEC, str, "PIL.Image.Image"]] = None,
        vector_column_name: str = VECTOR_COLUMN_NAME,
        query_type: str = "auto",
    ) -> LanceQueryBuilder:
        """Create a search query to find the nearest neighbors
        of the given query vector. We currently support [vector search][search]
        and [full-text search][search].

        Examples
        --------
        >>> import lancedb
        >>> db = lancedb.connect("./.lancedb")
        >>> data = [
        ...    {"original_width": 100, "caption": "bar", "vector": [0.1, 2.3, 4.5]},
        ...    {"original_width": 2000, "caption": "foo",  "vector": [0.5, 3.4, 1.3]},
        ...    {"original_width": 3000, "caption": "test", "vector": [0.3, 6.2, 2.6]}
        ... ]
        >>> table = db.create_table("my_table", data)
        >>> query = [0.4, 1.4, 2.4]
        >>> (table.search(query, vector_column_name="vector")
        ...     .where("original_width > 1000", prefilter=True)
        ...     .select(["caption", "original_width"])
        ...     .limit(2)
        ...     .to_pandas())
          caption  original_width           vector  _distance
        0     foo            2000  [0.5, 3.4, 1.3]   5.220000
        1    test            3000  [0.3, 6.2, 2.6]  23.089996

        Parameters
        ----------
        query: list/np.ndarray/str/PIL.Image.Image, default None
            The targetted vector to search for.

            - *default None*.
            Acceptable types are: list, np.ndarray, PIL.Image.Image

            - If None then the select/[where][sql]/limit clauses are applied
            to filter the table
        vector_column_name: str, default "vector"
            The name of the vector column to search.
        query_type: str, default "auto"
            "vector", "fts", or "auto"
            If "auto" then the query type is inferred from the query;
            If `query` is a list/np.ndarray then the query type is "vector";
            If `query` is a PIL.Image.Image then either do vector search
            or raise an error if no corresponding embedding function is found.
            If the `query` is a string, then the query type is "vector" if the
            table has embedding functions, else the query type is "fts"

        Returns
        -------
        LanceQueryBuilder
            A query builder object representing the query.
            Once executed, the query returns selected columns, the vector,
            and also the "_distance" column which is the distance between the query
            vector and the returned vector.
        """
        register_event("search_table")
        return LanceQueryBuilder.create(
            self, query, query_type, vector_column_name=vector_column_name
        )

    @classmethod
    def create(
        cls,
        db,
        name,
        data=None,
        schema=None,
        mode="create",
        exist_ok=False,
        on_bad_vectors: str = "error",
        fill_value: float = 0.0,
        embedding_functions: List[EmbeddingFunctionConfig] = None,
    ):
        """
        Create a new table.

        Examples
        --------
        >>> import lancedb
        >>> data = [
        ...    {"x": 1, "vector": [1, 2]},
        ...    {"x": 2, "vector": [3, 4]},
        ...    {"x": 3, "vector": [5, 6]}
        ... ]
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
        schema: pa.Schema or LanceModel, optional
            The schema of the table. If not provided,
            the schema is inferred from the data.
            At least one of `data` or `schema` must be provided.
        mode: str, default "create"
            The mode to use when writing the data. Valid values are
            "create", "overwrite", and "append".
        exist_ok: bool, default False
            If the table already exists then raise an error if False,
            otherwise just open the table, it will not add the provided
            data but will validate against any schema that's specified.
        on_bad_vectors: str, default "error"
            What to do if any of the vectors are not the same size or contains NaNs.
            One of "error", "drop", "fill".
        fill_value: float, default 0.
            The value to use when filling vectors. Only used if on_bad_vectors="fill".
        embedding_functions: list of EmbeddingFunctionModel, default None
            The embedding functions to use when creating the table.
        """
        tbl = LanceTable(db, name)
        if inspect.isclass(schema) and issubclass(schema, LanceModel):
            # convert LanceModel to pyarrow schema
            # note that it's possible this contains
            # embedding function metadata already
            schema = schema.to_arrow_schema()

        metadata = None
        if embedding_functions is not None:
            # If we passed in embedding functions explicitly
            # then we'll override any schema metadata that
            # may was implicitly specified by the LanceModel schema
            registry = EmbeddingFunctionRegistry.get_instance()
            metadata = registry.get_table_metadata(embedding_functions)

        if data is not None:
            data = _sanitize_data(
                data,
                schema,
                metadata=metadata,
                on_bad_vectors=on_bad_vectors,
                fill_value=fill_value,
            )

        if schema is None:
            if data is None:
                raise ValueError("Either data or schema must be provided")
            elif hasattr(data, "schema"):
                schema = data.schema
            elif isinstance(data, Iterable):
                if metadata:
                    raise TypeError(
                        (
                            "Persistent embedding functions not yet "
                            "supported for generator data input"
                        )
                    )

        if metadata:
            schema = schema.with_metadata(metadata)

        empty = pa.Table.from_pylist([], schema=schema)
        try:
            lance.write_dataset(empty, tbl._dataset_uri, schema=schema, mode=mode)
        except OSError as err:
            if "Dataset already exists" in str(err) and exist_ok:
                if tbl.schema != schema:
                    raise ValueError(
                        f"Table {name} already exists with a different schema"
                    )
                return tbl
            raise

        new_table = LanceTable(db, name)

        if data is not None:
            new_table.add(data)

        register_event("create_table")
        return new_table

    @classmethod
    def open(cls, db, name):
        tbl = cls(db, name)
        fs, path = fs_from_uri(tbl._dataset_uri)
        file_info = fs.get_file_info(path)
        if file_info.type != pa.fs.FileType.Directory:
            raise FileNotFoundError(
                f"Table {name} does not exist."
                f"Please first call db.create_table({name}, data)"
            )
        register_event("open_table")

        return tbl

    def delete(self, where: str):
        self._dataset.delete(where)

    def update(
        self,
        where: Optional[str] = None,
        values: Optional[dict] = None,
        *,
        values_sql: Optional[Dict[str, str]] = None,
    ):
        """
        This can be used to update zero to all rows depending on how many
        rows match the where clause.

        Parameters
        ----------
        where: str, optional
            The SQL where clause to use when updating rows. For example, 'x = 2'
            or 'x IN (1, 2, 3)'. The filter must not be empty, or it will error.
        values: dict, optional
            The values to update. The keys are the column names and the values
            are the values to set.
        values_sql: dict, optional
            The values to update, expressed as SQL expression strings. These can
            reference existing columns. For example, {"x": "x + 1"} will increment
            the x column by 1.

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
        >>> table.update(where="x = 2", values={"vector": [10, 10]})
        >>> table.to_pandas()
           x        vector
        0  1    [1.0, 2.0]
        1  3    [5.0, 6.0]
        2  2  [10.0, 10.0]

        """
        if values is not None and values_sql is not None:
            raise ValueError("Only one of values or values_sql can be provided")
        if values is None and values_sql is None:
            raise ValueError("Either values or values_sql must be provided")

        if values is not None:
            values_sql = {k: value_to_sql(v) for k, v in values.items()}

        self.to_lance().update(values_sql, where)
        self._reset_dataset()
        register_event("update")

    def _execute_query(self, query: Query) -> pa.Table:
        ds = self.to_lance()
        return ds.to_table(
            columns=query.columns,
            filter=query.filter,
            prefilter=query.prefilter,
            nearest={
                "column": query.vector_column,
                "q": query.vector,
                "k": query.k,
                "metric": query.metric,
                "nprobes": query.nprobes,
                "refine_factor": query.refine_factor,
            },
        )

    def cleanup_old_versions(
        self,
        older_than: Optional[timedelta] = None,
        *,
        delete_unverified: bool = False,
    ) -> CleanupStats:
        """
        Clean up old versions of the table, freeing disk space.

        Parameters
        ----------
        older_than: timedelta, default None
            The minimum age of the version to delete. If None, then this defaults
            to two weeks.
        delete_unverified: bool, default False
            Because they may be part of an in-progress transaction, files newer
            than 7 days old are not deleted by default. If you are sure that
            there are no in-progress transactions, then you can set this to True
            to delete all files older than `older_than`.

        Returns
        -------
        CleanupStats
            The stats of the cleanup operation, including how many bytes were
            freed.
        """
        return self.to_lance().cleanup_old_versions(
            older_than, delete_unverified=delete_unverified
        )

    def compact_files(self, *args, **kwargs):
        """
        Run the compaction process on the table.

        This can be run after making several small appends to optimize the table
        for faster reads.

        Arguments are passed onto :meth:`lance.dataset.DatasetOptimizer.compact_files`.
        For most cases, the default should be fine.
        """
        return self.to_lance().optimize.compact_files(*args, **kwargs)


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
        for field in schema:
            # TODO: we're making an assumption that fixed size list of 10 or more
            # is a vector column. This is definitely a bit hacky.
            likely_vector_col = (
                pa.types.is_fixed_size_list(field.type)
                and pa.types.is_float32(field.type.value_type)
                and field.type.list_size >= 10
            )
            is_default_vector_col = field.name == VECTOR_COLUMN_NAME
            if field.name in data.column_names and (
                likely_vector_col or is_default_vector_col
            ):
                data = _sanitize_vector_column(
                    data,
                    vector_column_name=field.name,
                    on_bad_vectors=on_bad_vectors,
                    fill_value=fill_value,
                )
        return pa.Table.from_arrays(
            [data[name] for name in schema.names], schema=schema
        )

    # just check the vector column
    if VECTOR_COLUMN_NAME in data.column_names:
        return _sanitize_vector_column(
            data,
            vector_column_name=VECTOR_COLUMN_NAME,
            on_bad_vectors=on_bad_vectors,
            fill_value=fill_value,
        )

    return data


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
    # ChunkedArray is annoying to work with, so we combine chunks here
    vec_arr = data[vector_column_name].combine_chunks()
    typ = data[vector_column_name].type
    if pa.types.is_list(typ) or pa.types.is_large_list(typ):
        # if it's a variable size list array,
        # we make sure the dimensions are all the same
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
