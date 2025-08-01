# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

from __future__ import annotations

import asyncio
import inspect
import deprecation
import warnings
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime, timedelta
from functools import cached_property
from typing import (
    TYPE_CHECKING,
    Any,
    Dict,
    Iterable,
    List,
    Literal,
    Optional,
    Tuple,
    Union,
    overload,
)
from urllib.parse import urlparse

from . import __version__
from lancedb.arrow import peek_reader
from lancedb.background_loop import LOOP
from .dependencies import (
    _check_for_hugging_face,
    _check_for_lance,
    _check_for_pandas,
    lance,
    pandas as pd,
    polars as pl,
)
import pyarrow as pa
import pyarrow.dataset
import pyarrow.compute as pc
import pyarrow.fs as pa_fs
import numpy as np

from .common import DATA, VEC, VECTOR_COLUMN_NAME
from .embeddings import EmbeddingFunctionConfig, EmbeddingFunctionRegistry
from .index import BTree, IvfFlat, IvfPq, Bitmap, LabelList, HnswPq, HnswSq, FTS
from .merge import LanceMergeInsertBuilder
from .pydantic import LanceModel, model_to_dict
from .query import (
    AsyncFTSQuery,
    AsyncHybridQuery,
    AsyncQuery,
    AsyncVectorQuery,
    FullTextQuery,
    LanceEmptyQueryBuilder,
    LanceFtsQueryBuilder,
    LanceHybridQueryBuilder,
    LanceQueryBuilder,
    LanceVectorQueryBuilder,
    Query,
)
from .util import (
    add_note,
    fs_from_uri,
    get_uri_scheme,
    infer_vector_column_name,
    join_uri,
    value_to_sql,
)
from .index import lang_mapping


if TYPE_CHECKING:
    from ._lancedb import (
        Table as LanceDBTable,
        OptimizeStats,
        CleanupStats,
        CompactionStats,
        Tag,
        AddColumnsResult,
        AddResult,
        AlterColumnsResult,
        DeleteResult,
        DropColumnsResult,
        MergeResult,
        UpdateResult,
    )
    from .db import LanceDBConnection
    from .index import IndexConfig
    import pandas
    import PIL
    from .types import (
        QueryType,
        OnBadVectorsType,
        AddMode,
        CreateMode,
        VectorIndexType,
        ScalarIndexType,
        BaseTokenizerType,
        DistanceType,
    )


def _into_pyarrow_reader(
    data, schema: Optional[pa.Schema] = None
) -> pa.RecordBatchReader:
    from lancedb.dependencies import datasets

    if _check_for_hugging_face(data):
        if isinstance(data, datasets.Dataset):
            schema = data.features.arrow_schema
            return pa.RecordBatchReader.from_batches(schema, data.data.to_batches())
        elif isinstance(data, datasets.dataset_dict.DatasetDict):
            schema = _schema_from_hf(data, None)
            if "split" not in schema.names:
                schema = schema.append(pa.field("split", pa.string()))
            return pa.RecordBatchReader.from_batches(
                schema, _to_batches_with_split(data)
            )
    if isinstance(data, LanceModel):
        raise ValueError("Cannot add a single LanceModel to a table. Use a list.")

    if isinstance(data, dict):
        raise ValueError("Cannot add a single dictionary to a table. Use a list.")

    if isinstance(data, list):
        # Handle empty list case
        if not data:
            if schema is None:
                raise ValueError("Cannot create table from empty list without a schema")
            return pa.Table.from_pylist(data, schema=schema).to_reader()

        # convert to list of dict if data is a bunch of LanceModels
        if isinstance(data[0], LanceModel):
            schema = data[0].__class__.to_arrow_schema()
            data = [model_to_dict(d) for d in data]
            return pa.Table.from_pylist(data, schema=schema).to_reader()
        elif isinstance(data[0], pa.RecordBatch):
            return pa.Table.from_batches(data).to_reader()
        else:
            return pa.Table.from_pylist(data).to_reader()
    elif _check_for_pandas(data) and isinstance(data, pd.DataFrame):
        table = pa.Table.from_pandas(data, preserve_index=False)
        # Do not serialize Pandas metadata
        meta = table.schema.metadata if table.schema.metadata is not None else {}
        meta = {k: v for k, v in meta.items() if k != b"pandas"}
        return table.replace_schema_metadata(meta).to_reader()
    elif isinstance(data, pa.Table):
        return data.to_reader()
    elif isinstance(data, pa.RecordBatch):
        return pa.RecordBatchReader.from_batches(data.schema, [data])
    elif _check_for_lance(data) and isinstance(data, lance.LanceDataset):
        return data.scanner().to_reader()
    elif isinstance(data, pa.dataset.Dataset):
        return data.scanner().to_reader()
    elif isinstance(data, pa.dataset.Scanner):
        return data.to_reader()
    elif isinstance(data, pa.RecordBatchReader):
        return data
    elif (
        type(data).__module__.startswith("polars")
        and data.__class__.__name__ == "DataFrame"
    ):
        return data.to_arrow().to_reader()
    elif (
        type(data).__module__.startswith("polars")
        and data.__class__.__name__ == "LazyFrame"
    ):
        return data.collect().to_arrow().to_reader()
    elif isinstance(data, Iterable):
        return _iterator_to_reader(data)
    else:
        raise TypeError(
            f"Unknown data type {type(data)}. "
            "Supported types: list of dicts, pandas DataFrame, polars DataFrame, "
            "pyarrow Table/RecordBatch, or Pydantic models. "
            "See https://lancedb.github.io/lancedb/guides/tables/ for examples."
        )


def _iterator_to_reader(data: Iterable) -> pa.RecordBatchReader:
    # Each batch is treated as it's own reader, mainly so we can
    # re-use the _into_pyarrow_reader logic.
    first = _into_pyarrow_reader(next(data))
    schema = first.schema

    def gen():
        yield from first
        for batch in data:
            table: pa.Table = _into_pyarrow_reader(batch).read_all()
            if table.schema != schema:
                try:
                    table = table.cast(schema)
                except pa.lib.ArrowInvalid:
                    raise ValueError(
                        f"Input iterator yielded a batch with schema that "
                        f"does not match the schema of other batches.\n"
                        f"Expected:\n{schema}\nGot:\n{batch.schema}"
                    )
            yield from table.to_batches()

    return pa.RecordBatchReader.from_batches(schema, gen())


def _sanitize_data(
    data: "DATA",
    target_schema: Optional[pa.Schema] = None,
    metadata: Optional[dict] = None,  # embedding metadata
    on_bad_vectors: OnBadVectorsType = "error",
    fill_value: float = 0.0,
    *,
    allow_subschema: bool = False,
) -> pa.RecordBatchReader:
    """
    Handle input data, applying all standard transformations.

    This includes:

     * Converting the data to a PyArrow Table
     * Adding vector columns defined in the metadata
     * Adding embedding metadata into the schema
     * Casting the table to the target schema
     * Handling bad vectors

    Parameters
    ----------
    target_schema : Optional[pa.Schema], default None
        The schema to cast the table to. This is typically the schema of the table
        if it already exists. Otherwise it might be a user-requested schema.
    allow_subschema : bool, default False
        If True, the input table is allowed to omit columns from the target schema.
        The target schema will be filtered to only include columns that are present
        in the input table before casting.
    metadata : Optional[dict], default None
        The embedding metadata to add to the schema.
    on_bad_vectors : Literal["error", "drop", "fill", "null"], default "error"
        What to do if any of the vectors are not the same size or contains NaNs.
    fill_value : float, default 0.0
        The value to use when filling vectors. Only used if on_bad_vectors="fill".
        All entries in the vector will be set to this value.
    """
    # At this point, the table might not match the schema we are targeting:
    # 1. There might be embedding columns missing that will be added
    #    in the add_embeddings step.
    # 2. If `allow_subschemas` is True, there might be columns missing.
    reader = _into_pyarrow_reader(data, target_schema)

    reader = _append_vector_columns(reader, target_schema, metadata=metadata)

    # This happens before the cast so we can fix vector columns with
    # incorrect lengths before they are cast to FSL.
    reader = _handle_bad_vectors(
        reader,
        on_bad_vectors=on_bad_vectors,
        fill_value=fill_value,
    )

    if target_schema is None:
        target_schema, reader = _infer_target_schema(reader)

    if metadata:
        new_metadata = target_schema.metadata or {}
        new_metadata = new_metadata.update(metadata)
        target_schema = target_schema.with_metadata(new_metadata)

    _validate_schema(target_schema)
    reader = _cast_to_target_schema(reader, target_schema, allow_subschema)

    return reader


def _cast_to_target_schema(
    reader: pa.RecordBatchReader,
    target_schema: pa.Schema,
    allow_subschema: bool = False,
) -> pa.RecordBatchReader:
    # pa.Table.cast expects field order not to be changed.
    # Lance doesn't care about field order, so we don't need to rearrange fields
    # to match the target schema. We just need to correctly cast the fields.
    if reader.schema == target_schema:
        # Fast path when the schemas are already the same
        return reader

    fields = _align_field_types(list(iter(reader.schema)), list(iter(target_schema)))
    reordered_schema = pa.schema(fields, metadata=target_schema.metadata)
    if not allow_subschema and len(reordered_schema) != len(target_schema):
        raise ValueError(
            "Input table has different number of columns than target schema"
        )

    if allow_subschema and len(reordered_schema) != len(target_schema):
        fields = _infer_subschema(
            list(iter(reader.schema)), list(iter(reordered_schema))
        )
        reordered_schema = pa.schema(fields, metadata=target_schema.metadata)

    def gen():
        for batch in reader:
            # Table but not RecordBatch has cast.
            yield pa.Table.from_batches([batch]).cast(reordered_schema).to_batches()[0]

    return pa.RecordBatchReader.from_batches(reordered_schema, gen())


def _align_field_types(
    fields: List[pa.Field],
    target_fields: List[pa.Field],
) -> List[pa.Field]:
    """
    Apply the data types from the target_fields to the fields.
    """
    new_fields = []
    for field in fields:
        target_field = next((f for f in target_fields if f.name == field.name), None)
        if target_field is None:
            raise ValueError(f"Field '{field.name}' not found in target schema")
        if pa.types.is_struct(target_field.type):
            new_type = pa.struct(
                _align_field_types(
                    field.type.fields,
                    target_field.type.fields,
                )
            )
        elif pa.types.is_list(target_field.type):
            new_type = pa.list_(
                _align_field_types(
                    [field.type.value_field],
                    [target_field.type.value_field],
                )[0]
            )
        elif pa.types.is_large_list(target_field.type):
            new_type = pa.large_list(
                _align_field_types(
                    [field.type.value_field],
                    [target_field.type.value_field],
                )[0]
            )
        elif pa.types.is_fixed_size_list(target_field.type):
            new_type = pa.list_(
                _align_field_types(
                    [field.type.value_field],
                    [target_field.type.value_field],
                )[0],
                target_field.type.list_size,
            )
        else:
            new_type = target_field.type
        new_fields.append(pa.field(field.name, new_type, field.nullable))
    return new_fields


def _infer_subschema(
    schema: List[pa.Field],
    reference_fields: List[pa.Field],
) -> List[pa.Field]:
    """
    Transform the list of fields so the types match the reference_fields.

    The order of the fields is preserved.

    ``schema`` may have fewer fields than `reference_fields`, but it may not have
    more fields.

    """
    fields = []
    lookup = {f.name: f for f in reference_fields}
    for field in schema:
        reference = lookup.get(field.name)
        if reference is None:
            raise ValueError("Unexpected field in schema: {}".format(field))

        if pa.types.is_struct(reference.type):
            new_type = pa.struct(
                _infer_subschema(
                    field.type.fields,
                    reference.type.fields,
                )
            )
            new_field = pa.field(
                field.name,
                new_type,
                reference.nullable,
            )
        else:
            new_field = reference

        fields.append(new_field)

    return fields


def sanitize_create_table(
    data,
    schema: Union[pa.Schema, LanceModel],
    metadata=None,
    on_bad_vectors: OnBadVectorsType = "error",
    fill_value: float = 0.0,
):
    if inspect.isclass(schema) and issubclass(schema, LanceModel):
        # convert LanceModel to pyarrow schema
        # note that it's possible this contains
        # embedding function metadata already
        schema: pa.Schema = schema.to_arrow_schema()

    if data is not None:
        if metadata is None and schema is not None:
            metadata = schema.metadata
        data = _sanitize_data(
            data,
            schema,
            metadata=metadata,
            on_bad_vectors=on_bad_vectors,
            fill_value=fill_value,
        )
        schema = data.schema
    else:
        if schema is not None:
            data = pa.Table.from_pylist([], schema)
    if schema is None:
        if data is None:
            raise ValueError("Either data or schema must be provided")
        elif hasattr(data, "schema"):
            schema = data.schema

    if metadata:
        schema = schema.with_metadata(metadata)
        # Need to apply metadata to the data as well
        if isinstance(data, pa.Table):
            data = data.replace_schema_metadata(metadata)
        elif isinstance(data, pa.RecordBatchReader):
            data = pa.RecordBatchReader.from_batches(schema, data)

    return data, schema


def _schema_from_hf(data, schema) -> pa.Schema:
    """
    Extract pyarrow schema from HuggingFace DatasetDict
    and validate that they're all the same schema between
    splits
    """
    for dataset in data.values():
        if schema is None:
            schema = dataset.features.arrow_schema
        elif schema != dataset.features.arrow_schema:
            msg = "All datasets in a HuggingFace DatasetDict must have the same schema"
            raise TypeError(msg)
    return schema


def _to_batches_with_split(data):
    """
    Return a generator of RecordBatches from a HuggingFace DatasetDict
    with an extra `split` column
    """
    for key, dataset in data.items():
        for batch in dataset.data.to_batches():
            table = pa.Table.from_batches([batch])
            if "split" not in table.column_names:
                table = table.append_column(
                    "split", pa.array([key] * batch.num_rows, pa.string())
                )
            for b in table.to_batches():
                yield b


def _append_vector_columns(
    reader: pa.RecordBatchReader,
    schema: Optional[pa.Schema] = None,
    *,
    metadata: Optional[dict] = None,
) -> pa.RecordBatchReader:
    """
    Use the embedding function to automatically embed the source columns and add the
    vector columns to the table.
    """
    if schema is None:
        metadata = metadata or {}
    else:
        metadata = schema.metadata or metadata or {}
    functions = EmbeddingFunctionRegistry.get_instance().parse_functions(metadata)

    if not functions:
        return reader

    fields = list(reader.schema)
    for vector_column, conf in functions.items():
        if vector_column not in reader.schema.names:
            if schema is not None:
                field = schema.field(vector_column)
            else:
                dtype = pa.list_(pa.float32(), conf.function.ndims())
                field = pa.field(vector_column, type=dtype, nullable=True)
            fields.append(field)
    schema = pa.schema(fields, metadata=reader.schema.metadata)

    def gen():
        for batch in reader:
            for vector_column, conf in functions.items():
                func = conf.function
                no_vector_column = vector_column not in batch.column_names
                if no_vector_column or pc.all(pc.is_null(batch[vector_column])).as_py():
                    col_data = func.compute_source_embeddings_with_retry(
                        batch[conf.source_column]
                    )
                    if no_vector_column:
                        batch = batch.append_column(
                            schema.field(vector_column),
                            pa.array(col_data, type=schema.field(vector_column).type),
                        )
                    else:
                        batch = batch.set_column(
                            batch.column_names.index(vector_column),
                            schema.field(vector_column),
                            pa.array(col_data, type=schema.field(vector_column).type),
                        )
            yield batch

    return pa.RecordBatchReader.from_batches(schema, gen())


def _table_path(base: str, table_name: str) -> str:
    """
    Get a table path that can be used in PyArrow FS.

    Removes any weird schemes (such as "s3+ddb") and drops any query params.
    """
    uri = _table_uri(base, table_name)
    # Parse as URL
    parsed = urlparse(uri)
    # If scheme is s3+ddb, convert to s3
    if parsed.scheme == "s3+ddb":
        parsed = parsed._replace(scheme="s3")
    # Remove query parameters
    return parsed._replace(query=None).geturl()


def _table_uri(base: str, table_name: str) -> str:
    return join_uri(base, f"{table_name}.lance")


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
    AddResult(version=2)

    Can query the table with [Table.search][lancedb.table.Table.search].

    >>> table.search([0.4, 0.4]).select(["b", "vector"]).to_pandas()
       b      vector  _distance
    0  4  [0.5, 1.3]       0.82
    1  2  [1.1, 1.2]       1.13

    Search queries are much faster when an index is created. See
    [Table.create_index][lancedb.table.Table.create_index].
    """

    @property
    @abstractmethod
    def name(self) -> str:
        """The name of this Table"""
        raise NotImplementedError

    @property
    @abstractmethod
    def version(self) -> int:
        """The version of this Table"""
        raise NotImplementedError

    @property
    @abstractmethod
    def schema(self) -> pa.Schema:
        """The [Arrow Schema](https://arrow.apache.org/docs/python/api/datatypes.html#)
        of this Table

        """
        raise NotImplementedError

    @property
    @abstractmethod
    def tags(self) -> Tags:
        """Tag management for the table.

        Similar to Git, tags are a way to add metadata to a specific version of the
        table.

        .. warning::

            Tagged versions are exempted from the :py:meth:`cleanup_old_versions()`
            process.

            To remove a version that has been tagged, you must first
            :py:meth:`~Tags.delete` the associated tag.

        Examples
        --------

        .. code-block:: python

            table = db.open_table("my_table")
            table.tags.create("v2-prod-20250203", 10)

            tags = table.tags.list()

        """
        raise NotImplementedError

    def __len__(self) -> int:
        """The number of rows in this Table"""
        return self.count_rows(None)

    @property
    @abstractmethod
    def embedding_functions(self) -> Dict[str, EmbeddingFunctionConfig]:
        """
        Get a mapping from vector column name to it's configured embedding function.
        """

    @abstractmethod
    def count_rows(self, filter: Optional[str] = None) -> int:
        """
        Count the number of rows in the table.

        Parameters
        ----------
        filter: str, optional
            A SQL where clause to filter the rows to count.
        """
        raise NotImplementedError

    def to_pandas(self) -> "pandas.DataFrame":
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
        metric="l2",
        num_partitions=256,
        num_sub_vectors=96,
        vector_column_name: str = VECTOR_COLUMN_NAME,
        replace: bool = True,
        accelerator: Optional[str] = None,
        index_cache_size: Optional[int] = None,
        *,
        index_type: VectorIndexType = "IVF_PQ",
        wait_timeout: Optional[timedelta] = None,
        num_bits: int = 8,
        max_iterations: int = 50,
        sample_rate: int = 256,
        m: int = 20,
        ef_construction: int = 300,
    ):
        """Create an index on the table.

        Parameters
        ----------
        metric: str, default "l2"
            The distance metric to use when creating the index.
            Valid values are "l2", "cosine", "dot", or "hamming".
            l2 is euclidean distance.
            Hamming is available only for binary vectors.
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
        num_bits: int
            The number of bits to encode sub-vectors. Only used with the IVF_PQ index.
            Only 4 and 8 are supported.
        wait_timeout: timedelta, optional
            The timeout to wait if indexing is asynchronous.
        """
        raise NotImplementedError

    def drop_index(self, name: str) -> None:
        """
        Drop an index from the table.

        Parameters
        ----------
        name: str
            The name of the index to drop.

        Notes
        -----
        This does not delete the index from disk, it just removes it from the table.
        To delete the index, run [optimize][lancedb.table.Table.optimize]
        after dropping the index.

        Use [list_indices][lancedb.table.Table.list_indices] to find the names of
        the indices.
        """
        raise NotImplementedError

    def wait_for_index(
        self, index_names: Iterable[str], timeout: timedelta = timedelta(seconds=300)
    ) -> None:
        """
        Wait for indexing to complete for the given index names.
        This will poll the table until all the indices are fully indexed,
        or raise a timeout exception if the timeout is reached.

        Parameters
        ----------
        index_names: str
            The name of the indices to poll
        timeout: timedelta
            Timeout to wait for asynchronous indexing. The default is 5 minutes.
        """
        raise NotImplementedError

    @abstractmethod
    def stats(self) -> TableStatistics:
        """
        Retrieve table and fragment statistics.
        """
        raise NotImplementedError

    @abstractmethod
    def create_scalar_index(
        self,
        column: str,
        *,
        replace: bool = True,
        index_type: ScalarIndexType = "BTREE",
        wait_timeout: Optional[timedelta] = None,
    ):
        """Create a scalar index on a column.

        Parameters
        ----------
        column : str
            The column to be indexed.  Must be a boolean, integer, float,
            or string column.
        replace : bool, default True
            Replace the existing index if it exists.
        index_type: Literal["BTREE", "BITMAP", "LABEL_LIST"], default "BTREE"
            The type of index to create.
        wait_timeout: timedelta, optional
            The timeout to wait if indexing is asynchronous.
        Examples
        --------

        Scalar indices, like vector indices, can be used to speed up scans.  A scalar
        index can speed up scans that contain filter expressions on the indexed column.
        For example, the following scan will be faster if the column ``my_col`` has
        a scalar index:

        >>> import lancedb # doctest: +SKIP
        >>> db = lancedb.connect("/data/lance") # doctest: +SKIP
        >>> img_table = db.open_table("images") # doctest: +SKIP
        >>> my_df = img_table.search().where("my_col = 7", # doctest: +SKIP
        ...                                  prefilter=True).to_pandas()

        Scalar indices can also speed up scans containing a vector search and a
        prefilter:

        >>> import lancedb # doctest: +SKIP
        >>> db = lancedb.connect("/data/lance") # doctest: +SKIP
        >>> img_table = db.open_table("images") # doctest: +SKIP
        >>> img_table.search([1, 2, 3, 4], vector_column_name="vector") # doctest: +SKIP
        ...     .where("my_col != 7", prefilter=True)
        ...     .to_pandas()

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
        """
        raise NotImplementedError

    def create_fts_index(
        self,
        field_names: Union[str, List[str]],
        *,
        ordering_field_names: Optional[Union[str, List[str]]] = None,
        replace: bool = False,
        writer_heap_size: Optional[int] = 1024 * 1024 * 1024,
        use_tantivy: bool = False,
        tokenizer_name: Optional[str] = None,
        with_position: bool = False,
        # tokenizer configs:
        base_tokenizer: BaseTokenizerType = "simple",
        language: str = "English",
        max_token_length: Optional[int] = 40,
        lower_case: bool = True,
        stem: bool = True,
        remove_stop_words: bool = True,
        ascii_folding: bool = True,
        ngram_min_length: int = 3,
        ngram_max_length: int = 3,
        prefix_only: bool = False,
        wait_timeout: Optional[timedelta] = None,
    ):
        """Create a full-text search index on the table.

        Warning - this API is highly experimental and is highly likely to change
        in the future.

        Parameters
        ----------
        field_names: str or list of str
            The name(s) of the field to index.
            can be only str if use_tantivy=True for now.
        replace: bool, default False
            If True, replace the existing index if it exists. Note that this is
            not yet an atomic operation; the index will be temporarily
            unavailable while the new index is being created.
        writer_heap_size: int, default 1GB
            Only available with use_tantivy=True
        ordering_field_names:
            A list of unsigned type fields to index to optionally order
            results on at search time.
            only available with use_tantivy=True
        tokenizer_name: str, default "default"
            The tokenizer to use for the index. Can be "raw", "default" or the 2 letter
            language code followed by "_stem". So for english it would be "en_stem".
            For available languages see: https://docs.rs/tantivy/latest/tantivy/tokenizer/enum.Language.html
        use_tantivy: bool, default False
            If True, use the legacy full-text search implementation based on tantivy.
            If False, use the new full-text search implementation based on lance-index.
        with_position: bool, default False
            Only available with use_tantivy=False
            If False, do not store the positions of the terms in the text.
            This can reduce the size of the index and improve indexing speed.
            But it will raise an exception for phrase queries.
        base_tokenizer : str, default "simple"
            The base tokenizer to use for tokenization. Options are:
            - "simple": Splits text by whitespace and punctuation.
            - "whitespace": Split text by whitespace, but not punctuation.
            - "raw": No tokenization. The entire text is treated as a single token.
            - "ngram": N-Gram tokenizer.
        language : str, default "English"
            The language to use for tokenization.
        max_token_length : int, default 40
            The maximum token length to index. Tokens longer than this length will be
            ignored.
        lower_case : bool, default True
            Whether to convert the token to lower case. This makes queries
            case-insensitive.
        stem : bool, default True
            Whether to stem the token. Stemming reduces words to their root form.
            For example, in English "running" and "runs" would both be reduced to "run".
        remove_stop_words : bool, default True
            Whether to remove stop words. Stop words are common words that are often
            removed from text before indexing. For example, in English "the" and "and".
        ascii_folding : bool, default True
            Whether to fold ASCII characters. This converts accented characters to
            their ASCII equivalent. For example, "café" would be converted to "cafe".
        ngram_min_length: int, default 3
            The minimum length of an n-gram.
        ngram_max_length: int, default 3
            The maximum length of an n-gram.
        prefix_only: bool, default False
            Whether to only index the prefix of the token for ngram tokenizer.
        wait_timeout: timedelta, optional
            The timeout to wait if indexing is asynchronous.
        """
        raise NotImplementedError

    @abstractmethod
    def add(
        self,
        data: DATA,
        mode: AddMode = "append",
        on_bad_vectors: OnBadVectorsType = "error",
        fill_value: float = 0.0,
    ) -> AddResult:
        """Add more data to the [Table](Table).

        Parameters
        ----------
        data: DATA
            The data to insert into the table. Acceptable types are:

            - list-of-dict

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

        Returns
        -------
        AddResult
            An object containing the new version number of the table after adding data.
        """
        raise NotImplementedError

    def merge_insert(self, on: Union[str, Iterable[str]]) -> LanceMergeInsertBuilder:
        """
        Returns a [`LanceMergeInsertBuilder`][lancedb.merge.LanceMergeInsertBuilder]
        that can be used to create a "merge insert" operation

        This operation can add rows, update rows, and remove rows all in a single
        transaction. It is a very generic tool that can be used to create
        behaviors like "insert if not exists", "update or insert (i.e. upsert)",
        or even replace a portion of existing data with new data (e.g. replace
        all data where month="january")

        The merge insert operation works by combining new data from a
        **source table** with existing data in a **target table** by using a
        join.  There are three categories of records.

        "Matched" records are records that exist in both the source table and
        the target table. "Not matched" records exist only in the source table
        (e.g. these are new data) "Not matched by source" records exist only
        in the target table (this is old data)

        The builder returned by this method can be used to customize what
        should happen for each category of data.

        Please note that the data may appear to be reordered as part of this
        operation.  This is because updated rows will be deleted from the
        dataset and then reinserted at the end with the new values.

        Parameters
        ----------

        on: Union[str, Iterable[str]]
            A column (or columns) to join on.  This is how records from the
            source table and target table are matched.  Typically this is some
            kind of key or id column.

        Examples
        --------
        >>> import lancedb
        >>> data = pa.table({"a": [2, 1, 3], "b": ["a", "b", "c"]})
        >>> db = lancedb.connect("./.lancedb")
        >>> table = db.create_table("my_table", data)
        >>> new_data = pa.table({"a": [2, 3, 4], "b": ["x", "y", "z"]})
        >>> # Perform a "upsert" operation
        >>> res = table.merge_insert("a")     \\
        ...      .when_matched_update_all()     \\
        ...      .when_not_matched_insert_all() \\
        ...      .execute(new_data)
        >>> res
        MergeResult(version=2, num_updated_rows=2, num_inserted_rows=1, num_deleted_rows=0)
        >>> # The order of new rows is non-deterministic since we use
        >>> # a hash-join as part of this operation and so we sort here
        >>> table.to_arrow().sort_by("a").to_pandas()
           a  b
        0  1  b
        1  2  x
        2  3  y
        3  4  z
        """  # noqa: E501
        on = [on] if isinstance(on, str) else list(iter(on))

        return LanceMergeInsertBuilder(self, on)

    @abstractmethod
    def search(
        self,
        query: Optional[
            Union[VEC, str, "PIL.Image.Image", Tuple, FullTextQuery]
        ] = None,
        vector_column_name: Optional[str] = None,
        query_type: QueryType = "auto",
        ordering_field_name: Optional[str] = None,
        fts_columns: Optional[Union[str, List[str]]] = None,
    ) -> LanceQueryBuilder:
        """Create a search query to find the nearest neighbors
        of the given query vector. We currently support [vector search][search]
        and [full-text search][experimental-full-text-search].

        All query options are defined in
        [LanceQueryBuilder][lancedb.query.LanceQueryBuilder].

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
        >>> (table.search(query)
        ...     .where("original_width > 1000", prefilter=True)
        ...     .select(["caption", "original_width", "vector"])
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
        vector_column_name: str, optional
            The name of the vector column to search.

            The vector column needs to be a pyarrow fixed size list type

            - If not specified then the vector column is inferred from
            the table schema

            - If the table has multiple vector columns then the *vector_column_name*
            needs to be specified. Otherwise, an error is raised.
        query_type: str
            *default "auto"*.
            Acceptable types are: "vector", "fts", "hybrid", or "auto"

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
    def _execute_query(
        self,
        query: Query,
        *,
        batch_size: Optional[int] = None,
        timeout: Optional[timedelta] = None,
    ) -> pa.RecordBatchReader: ...

    @abstractmethod
    def _explain_plan(self, query: Query, verbose: Optional[bool] = False) -> str: ...

    @abstractmethod
    def _analyze_plan(self, query: Query) -> str: ...

    @abstractmethod
    def _do_merge(
        self,
        merge: LanceMergeInsertBuilder,
        new_data: DATA,
        on_bad_vectors: OnBadVectorsType,
        fill_value: float,
    ) -> MergeResult: ...

    @abstractmethod
    def delete(self, where: str) -> DeleteResult:
        """Delete rows from the table.

        This can be used to delete a single row, many rows, all rows, or
        sometimes no rows (if your predicate matches nothing).

        Parameters
        ----------
        where: str
            The SQL where clause to use when deleting rows.

            - For example, 'x = 2' or 'x IN (1, 2, 3)'.

            The filter must not be empty, or it will error.

        Returns
        -------
        DeleteResult
            An object containing the new version number of the table after deletion.

        Examples
        --------
        >>> import lancedb
        >>> data = [
        ...    {"x": 1, "vector": [1.0, 2]},
        ...    {"x": 2, "vector": [3.0, 4]},
        ...    {"x": 3, "vector": [5.0, 6]}
        ... ]
        >>> db = lancedb.connect("./.lancedb")
        >>> table = db.create_table("my_table", data)
        >>> table.to_pandas()
           x      vector
        0  1  [1.0, 2.0]
        1  2  [3.0, 4.0]
        2  3  [5.0, 6.0]
        >>> table.delete("x = 2")
        DeleteResult(version=2)
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
        DeleteResult(version=3)
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
    ) -> UpdateResult:
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

        Returns
        -------
        UpdateResult
            - rows_updated: The number of rows that were updated
            - version: The new version number of the table after the update

        Examples
        --------
        >>> import lancedb
        >>> import pandas as pd
        >>> data = pd.DataFrame({"x": [1, 2, 3], "vector": [[1.0, 2], [3, 4], [5, 6]]})
        >>> db = lancedb.connect("./.lancedb")
        >>> table = db.create_table("my_table", data)
        >>> table.to_pandas()
           x      vector
        0  1  [1.0, 2.0]
        1  2  [3.0, 4.0]
        2  3  [5.0, 6.0]
        >>> table.update(where="x = 2", values={"vector": [10.0, 10]})
        UpdateResult(rows_updated=1, version=2)
        >>> table.to_pandas()
           x        vector
        0  1    [1.0, 2.0]
        1  3    [5.0, 6.0]
        2  2  [10.0, 10.0]
        >>> table.update(values_sql={"x": "x + 1"})
        UpdateResult(rows_updated=3, version=3)
        >>> table.to_pandas()
           x        vector
        0  2    [1.0, 2.0]
        1  4    [5.0, 6.0]
        2  3  [10.0, 10.0]
        """
        raise NotImplementedError

    @abstractmethod
    def cleanup_old_versions(
        self,
        older_than: Optional[timedelta] = None,
        *,
        delete_unverified: bool = False,
    ) -> "CleanupStats":
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

        See Also
        --------
        [Table.optimize][lancedb.table.Table.optimize]: A more comprehensive
            optimization operation that includes cleanup as well as other operations.

        Notes
        -----
        This function is not available in LanceDb Cloud (since LanceDB
        Cloud manages cleanup for you automatically)
        """

    @abstractmethod
    def compact_files(self, *args, **kwargs):
        """
        Run the compaction process on the table.
        This can be run after making several small appends to optimize the table
        for faster reads.

        Arguments are passed onto Lance's
        [compact_files][lance.dataset.DatasetOptimizer.compact_files].
        For most cases, the default should be fine.

        See Also
        --------
        [Table.optimize][lancedb.table.Table.optimize]: A more comprehensive
            optimization operation that includes cleanup as well as other operations.

        Notes
        -----
        This function is not available in LanceDB Cloud (since LanceDB
        Cloud manages compaction for you automatically)
        """

    @abstractmethod
    def optimize(
        self,
        *,
        cleanup_older_than: Optional[timedelta] = None,
        delete_unverified: bool = False,
        retrain: bool = False,
    ):
        """
        Optimize the on-disk data and indices for better performance.

        Modeled after ``VACUUM`` in PostgreSQL.

        Optimization covers three operations:

         * Compaction: Merges small files into larger ones
         * Prune: Removes old versions of the dataset
         * Index: Optimizes the indices, adding new data to existing indices

        Parameters
        ----------
        cleanup_older_than: timedelta, optional default 7 days
            All files belonging to versions older than this will be removed.  Set
            to 0 days to remove all versions except the latest.  The latest version
            is never removed.
        delete_unverified: bool, default False
            Files leftover from a failed transaction may appear to be part of an
            in-progress operation (e.g. appending new data) and these files will not
            be deleted unless they are at least 7 days old. If delete_unverified is True
            then these files will be deleted regardless of their age.
        retrain: bool, default False
            If True, retrain the vector indices, this would refine the IVF clustering
            and quantization, which may improve the search accuracy. It's faster than
            re-creating the index from scratch, so it's recommended to try this first,
            when the data distribution has changed significantly.

        Experimental API
        ----------------

        The optimization process is undergoing active development and may change.
        Our goal with these changes is to improve the performance of optimization and
        reduce the complexity.

        That being said, it is essential today to run optimize if you want the best
        performance.  It should be stable and safe to use in production, but it our
        hope that the API may be simplified (or not even need to be called) in the
        future.

        The frequency an application shoudl call optimize is based on the frequency of
        data modifications.  If data is frequently added, deleted, or updated then
        optimize should be run frequently.  A good rule of thumb is to run optimize if
        you have added or modified 100,000 or more records or run more than 20 data
        modification operations.
        """

    @abstractmethod
    def list_indices(self) -> Iterable[IndexConfig]:
        """
        List all indices that have been created with
        [Table.create_index][lancedb.table.Table.create_index]
        """

    @abstractmethod
    def index_stats(self, index_name: str) -> Optional[IndexStatistics]:
        """
        Retrieve statistics about an index

        Parameters
        ----------
        index_name: str
            The name of the index to retrieve statistics for

        Returns
        -------
        IndexStatistics or None
            The statistics about the index. Returns None if the index does not exist.
        """

    @abstractmethod
    def add_columns(
        self, transforms: Dict[str, str] | pa.Field | List[pa.Field] | pa.Schema
    ):
        """
        Add new columns with defined values.

        Parameters
        ----------
        transforms: Dict[str, str], pa.Field, List[pa.Field], pa.Schema
            A map of column name to a SQL expression to use to calculate the
            value of the new column. These expressions will be evaluated for
            each row in the table, and can reference existing columns.
            Alternatively, a pyarrow Field or Schema can be provided to add
            new columns with the specified data types. The new columns will
            be initialized with null values.

        Returns
        -------
        AddColumnsResult
            version: the new version number of the table after adding columns.
        """

    @abstractmethod
    def alter_columns(self, *alterations: Iterable[Dict[str, str]]):
        """
        Alter column names and nullability.

        Parameters
        ----------
        alterations : Iterable[Dict[str, Any]]
            A sequence of dictionaries, each with the following keys:
            - "path": str
                The column path to alter. For a top-level column, this is the name.
                For a nested column, this is the dot-separated path, e.g. "a.b.c".
            - "rename": str, optional
                The new name of the column. If not specified, the column name is
                not changed.
            - "data_type": pyarrow.DataType, optional
               The new data type of the column. Existing values will be casted
               to this type. If not specified, the column data type is not changed.
            - "nullable": bool, optional
                Whether the column should be nullable. If not specified, the column
                nullability is not changed. Only non-nullable columns can be changed
                to nullable. Currently, you cannot change a nullable column to
                non-nullable.

        Returns
        -------
        AlterColumnsResult
            version: the new version number of the table after the alteration.
        """

    @abstractmethod
    def drop_columns(self, columns: Iterable[str]) -> DropColumnsResult:
        """
        Drop columns from the table.

        Parameters
        ----------
        columns : Iterable[str]
            The names of the columns to drop.

        Returns
        -------
        DropColumnsResult
            version: the new version number of the table dropping the columns.
        """

    @abstractmethod
    def checkout(self, version: Union[int, str]):
        """
        Checks out a specific version of the Table

        Any read operation on the table will now access the data at the checked out
        version. As a consequence, calling this method will disable any read consistency
        interval that was previously set.

        This is a read-only operation that turns the table into a sort of "view"
        or "detached head".  Other table instances will not be affected.  To make the
        change permanent you can use the `[Self::restore]` method.

        Any operation that modifies the table will fail while the table is in a checked
        out state.

        Parameters
        ----------
        version: int | str,
            The version to check out. A version number (`int`) or a tag
            (`str`) can be provided.

        To return the table to a normal state use `[Self::checkout_latest]`
        """

    @abstractmethod
    def checkout_latest(self):
        """
        Ensures the table is pointing at the latest version

        This can be used to manually update a table when the read_consistency_interval
        is None
        It can also be used to undo a `[Self::checkout]` operation
        """

    @abstractmethod
    def restore(self, version: Optional[Union[int, str]] = None):
        """Restore a version of the table. This is an in-place operation.

        This creates a new version where the data is equivalent to the
        specified previous version. Data is not copied (as of python-v0.2.1).

        Parameters
        ----------
        version : int or str, default None
            The version number or version tag to restore.
            If unspecified then restores the currently checked out version.
            If the currently checked out version is the
            latest version then this is a no-op.
        """

    @abstractmethod
    def list_versions(self) -> List[Dict[str, Any]]:
        """List all versions of the table"""

    @cached_property
    def _dataset_uri(self) -> str:
        return _table_uri(self._conn.uri, self.name)

    def _get_fts_index_path(self) -> Tuple[str, pa_fs.FileSystem, bool]:
        from .remote.table import RemoteTable

        if isinstance(self, RemoteTable) or get_uri_scheme(self._dataset_uri) != "file":
            return ("", None, False)
        path = join_uri(self._dataset_uri, "_indices", "fts")
        fs, path = fs_from_uri(path)
        index_exists = fs.get_file_info(path).type != pa_fs.FileType.NotFound
        return (path, fs, index_exists)

    @abstractmethod
    def uses_v2_manifest_paths(self) -> bool:
        """
        Check if the table is using the new v2 manifest paths.

        Returns
        -------
        bool
            True if the table is using the new v2 manifest paths, False otherwise.
        """

    @abstractmethod
    def migrate_v2_manifest_paths(self):
        """
        Migrate the manifest paths to the new format.

        This will update the manifest to use the new v2 format for paths.

        This function is idempotent, and can be run multiple times without
        changing the state of the object store.

        !!! danger

            This should not be run while other concurrent operations are happening.
            And it should also run until completion before resuming other operations.

        You can use
        [Table.uses_v2_manifest_paths][lancedb.table.Table.uses_v2_manifest_paths]
        to check if the table is already using the new path style.
        """


class LanceTable(Table):
    """
    A table in a LanceDB database.

    This can be opened in two modes: standard and time-travel.

    Standard mode is the default. In this mode, the table is mutable and tracks
    the latest version of the table. The level of read consistency is controlled
    by the `read_consistency_interval` parameter on the connection.

    Time-travel mode is activated by specifying a version number. In this mode,
    the table is immutable and fixed to a specific version. This is useful for
    querying historical versions of the table.
    """

    def __init__(
        self,
        connection: "LanceDBConnection",
        name: str,
        *,
        storage_options: Optional[Dict[str, str]] = None,
        index_cache_size: Optional[int] = None,
    ):
        self._conn = connection
        self._table = LOOP.run(
            connection._conn.open_table(
                name,
                storage_options=storage_options,
                index_cache_size=index_cache_size,
            )
        )

    @property
    def name(self) -> str:
        return self._table.name

    @classmethod
    def open(cls, db, name, **kwargs):
        tbl = cls(db, name, **kwargs)

        # check the dataset exists
        try:
            tbl.version
        except ValueError as e:
            if "Not found:" in str(e):
                raise FileNotFoundError(f"Table {name} does not exist")
            raise e

        return tbl

    @cached_property
    def _dataset_path(self) -> str:
        # Cacheable since it's deterministic
        return _table_path(self._conn.uri, self.name)

    def to_lance(self, **kwargs) -> lance.LanceDataset:
        """Return the LanceDataset backing this table."""
        try:
            import lance
        except ImportError:
            raise ImportError(
                "The lance library is required to use this function. "
                "Please install with `pip install pylance`."
            )

        return lance.dataset(
            self._dataset_path,
            version=self.version,
            storage_options=self._conn.storage_options,
            **kwargs,
        )

    @property
    def schema(self) -> pa.Schema:
        """Return the schema of the table.

        Returns
        -------
        pa.Schema
            A PyArrow schema object."""
        return LOOP.run(self._table.schema())

    def list_versions(self) -> List[Dict[str, Any]]:
        """List all versions of the table"""
        return LOOP.run(self._table.list_versions())

    @property
    def version(self) -> int:
        """Get the current version of the table"""
        return LOOP.run(self._table.version())

    @property
    def tags(self) -> Tags:
        """Tag management for the table.

        Similar to Git, tags are a way to add metadata to a specific version of the
        table.

        .. warning::

            Tagged versions are exempted from the :py:meth:`cleanup_old_versions()`
            process.

            To remove a version that has been tagged, you must first
            :py:meth:`~Tags.delete` the associated tag.

        Returns
        -------
        Tags
            The tag manager for managing tags for the table.

        Examples
        --------
        >>> import lancedb
        >>> db = lancedb.connect("./.lancedb")
        >>> table = db.create_table("my_table",
        ...    [{"vector": [1.1, 0.9], "type": "vector"}])
        >>> table.tags.create("v1", table.version)
        >>> table.add([{"vector": [0.5, 0.2], "type": "vector"}])
        AddResult(version=2)
        >>> tags = table.tags.list()
        >>> print(tags["v1"]["version"])
        1
        >>> table.checkout("v1")
        >>> table.to_pandas()
               vector    type
        0  [1.1, 0.9]  vector
        """
        return Tags(self._table)

    def checkout(self, version: Union[int, str]):
        """Checkout a version of the table. This is an in-place operation.

        This allows viewing previous versions of the table. If you wish to
        keep writing to the dataset starting from an old version, then use
        the `restore` function.

        Calling this method will set the table into time-travel mode. If you
        wish to return to standard mode, call `checkout_latest`.

        Parameters
        ----------
        version: int | str,
            The version to check out. A version number (`int`) or a tag
            (`str`) can be provided.

        Examples
        --------
        >>> import lancedb
        >>> db = lancedb.connect("./.lancedb")
        >>> table = db.create_table("my_table",
        ...    [{"vector": [1.1, 0.9], "type": "vector"}])
        >>> table.version
        1
        >>> table.to_pandas()
               vector    type
        0  [1.1, 0.9]  vector
        >>> table.add([{"vector": [0.5, 0.2], "type": "vector"}])
        AddResult(version=2)
        >>> table.version
        2
        >>> table.checkout(1)
        >>> table.to_pandas()
               vector    type
        0  [1.1, 0.9]  vector
        """
        LOOP.run(self._table.checkout(version))

    def checkout_latest(self):
        """Checkout the latest version of the table. This is an in-place operation.

        The table will be set back into standard mode, and will track the latest
        version of the table.
        """
        LOOP.run(self._table.checkout_latest())

    def restore(self, version: Optional[Union[int, str]] = None):
        """Restore a version of the table. This is an in-place operation.

        This creates a new version where the data is equivalent to the
        specified previous version. Data is not copied (as of python-v0.2.1).

        Parameters
        ----------
        version : int or str, default None
            The version number or version tag to restore.
            If unspecified then restores the currently checked out version.
            If the currently checked out version is the
            latest version then this is a no-op.

        Examples
        --------
        >>> import lancedb
        >>> db = lancedb.connect("./.lancedb")
        >>> table = db.create_table("my_table", [
        ...     {"vector": [1.1, 0.9], "type": "vector"}])
        >>> table.version
        1
        >>> table.to_pandas()
               vector    type
        0  [1.1, 0.9]  vector
        >>> table.add([{"vector": [0.5, 0.2], "type": "vector"}])
        AddResult(version=2)
        >>> table.version
        2
        >>> table.tags.create("v2", 2)
        >>> table.restore(1)
        >>> table.to_pandas()
               vector    type
        0  [1.1, 0.9]  vector
        >>> len(table.list_versions())
        3
        >>> table.restore("v2")
        >>> table.to_pandas()
               vector    type
        0  [1.1, 0.9]  vector
        1  [0.5, 0.2]  vector
        >>> len(table.list_versions())
        4
        """
        if version is not None:
            LOOP.run(self._table.checkout(version))
        LOOP.run(self._table.restore())

    def count_rows(self, filter: Optional[str] = None) -> int:
        return LOOP.run(self._table.count_rows(filter))

    def __repr__(self) -> str:
        val = f"{self.__class__.__name__}(name={self.name!r}, version={self.version}"
        if self._conn.read_consistency_interval is not None:
            val += ", read_consistency_interval={!r}".format(
                self._conn.read_consistency_interval
            )
        val += f", _conn={self._conn!r})"
        return val

    def __str__(self) -> str:
        return self.__repr__()

    def head(self, n=5) -> pa.Table:
        """Return the first n rows of the table."""
        return LOOP.run(self._table.head(n))

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
        return LOOP.run(self._table.to_arrow())

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
        from lancedb.integrations.pyarrow import PyarrowDatasetAdapter

        dataset = PyarrowDatasetAdapter(self)
        return pl.scan_pyarrow_dataset(
            dataset, allow_pyarrow_filter=False, batch_size=batch_size
        )

    def create_index(
        self,
        metric: DistanceType = "l2",
        num_partitions=None,
        num_sub_vectors=None,
        vector_column_name: str = VECTOR_COLUMN_NAME,
        replace: bool = True,
        accelerator: Optional[str] = None,
        index_cache_size: Optional[int] = None,
        num_bits: int = 8,
        index_type: Literal[
            "IVF_FLAT", "IVF_PQ", "IVF_HNSW_SQ", "IVF_HNSW_PQ"
        ] = "IVF_PQ",
        max_iterations: int = 50,
        sample_rate: int = 256,
        m: int = 20,
        ef_construction: int = 300,
    ):
        """Create an index on the table."""
        if accelerator is not None:
            # accelerator is only supported through pylance.
            self.to_lance().create_index(
                column=vector_column_name,
                index_type=index_type,
                metric=metric,
                num_partitions=num_partitions,
                num_sub_vectors=num_sub_vectors,
                replace=replace,
                accelerator=accelerator,
                index_cache_size=index_cache_size,
                num_bits=num_bits,
                m=m,
                ef_construction=ef_construction,
            )
            self.checkout_latest()
            return
        elif index_type == "IVF_FLAT":
            config = IvfFlat(
                distance_type=metric,
                num_partitions=num_partitions,
                max_iterations=max_iterations,
                sample_rate=sample_rate,
            )
        elif index_type == "IVF_PQ":
            config = IvfPq(
                distance_type=metric,
                num_partitions=num_partitions,
                num_sub_vectors=num_sub_vectors,
                num_bits=num_bits,
                max_iterations=max_iterations,
                sample_rate=sample_rate,
            )
        elif index_type == "IVF_HNSW_PQ":
            config = HnswPq(
                distance_type=metric,
                num_partitions=num_partitions,
                num_sub_vectors=num_sub_vectors,
                num_bits=num_bits,
                max_iterations=max_iterations,
                sample_rate=sample_rate,
                m=m,
                ef_construction=ef_construction,
            )
        elif index_type == "IVF_HNSW_SQ":
            config = HnswSq(
                distance_type=metric,
                num_partitions=num_partitions,
                max_iterations=max_iterations,
                sample_rate=sample_rate,
                m=m,
                ef_construction=ef_construction,
            )
        else:
            raise ValueError(f"Unknown index type {index_type}")

        return LOOP.run(
            self._table.create_index(
                vector_column_name,
                replace=replace,
                config=config,
            )
        )

    def drop_index(self, name: str) -> None:
        """
        Drops an index from the table

        Parameters
        ----------
        name: str
            The name of the index to drop
        """
        return LOOP.run(self._table.drop_index(name))

    def prewarm_index(self, name: str) -> None:
        """
        Prewarms an index in the table

        This loads the entire index into memory

        If the index does not fit into the available cache this call
        may be wasteful

        Parameters
        ----------
        name: str
            The name of the index to prewarm
        """
        return LOOP.run(self._table.prewarm_index(name))

    def wait_for_index(
        self, index_names: Iterable[str], timeout: timedelta = timedelta(seconds=300)
    ) -> None:
        return LOOP.run(self._table.wait_for_index(index_names, timeout))

    def stats(self) -> TableStatistics:
        return LOOP.run(self._table.stats())

    def create_scalar_index(
        self,
        column: str,
        *,
        replace: bool = True,
        index_type: ScalarIndexType = "BTREE",
    ):
        if index_type == "BTREE":
            config = BTree()
        elif index_type == "BITMAP":
            config = Bitmap()
        elif index_type == "LABEL_LIST":
            config = LabelList()
        else:
            raise ValueError(f"Unknown index type {index_type}")
        return LOOP.run(
            self._table.create_index(column, replace=replace, config=config)
        )

    def create_fts_index(
        self,
        field_names: Union[str, List[str]],
        *,
        ordering_field_names: Optional[Union[str, List[str]]] = None,
        replace: bool = False,
        writer_heap_size: Optional[int] = 1024 * 1024 * 1024,
        use_tantivy: bool = False,
        tokenizer_name: Optional[str] = None,
        with_position: bool = False,
        # tokenizer configs:
        base_tokenizer: BaseTokenizerType = "simple",
        language: str = "English",
        max_token_length: Optional[int] = 40,
        lower_case: bool = True,
        stem: bool = True,
        remove_stop_words: bool = True,
        ascii_folding: bool = True,
        ngram_min_length: int = 3,
        ngram_max_length: int = 3,
        prefix_only: bool = False,
    ):
        if not use_tantivy:
            if not isinstance(field_names, str):
                raise ValueError("field_names must be a string when use_tantivy=False")

            if tokenizer_name is None:
                tokenizer_configs = {
                    "base_tokenizer": base_tokenizer,
                    "language": language,
                    "with_position": with_position,
                    "max_token_length": max_token_length,
                    "lower_case": lower_case,
                    "stem": stem,
                    "remove_stop_words": remove_stop_words,
                    "ascii_folding": ascii_folding,
                    "ngram_min_length": ngram_min_length,
                    "ngram_max_length": ngram_max_length,
                    "prefix_only": prefix_only,
                }
            else:
                tokenizer_configs = self.infer_tokenizer_configs(tokenizer_name)

            config = FTS(
                **tokenizer_configs,
            )

            # delete the existing legacy index if it exists
            if replace:
                path, fs, exist = self._get_fts_index_path()
                if exist:
                    fs.delete_dir(path)

            LOOP.run(
                self._table.create_index(
                    field_names,
                    replace=replace,
                    config=config,
                )
            )
            return

        from .fts import create_index, populate_index

        if isinstance(field_names, str):
            field_names = [field_names]

        if isinstance(ordering_field_names, str):
            ordering_field_names = [ordering_field_names]

        path, fs, exist = self._get_fts_index_path()
        if exist:
            if not replace:
                raise ValueError("Index already exists. Use replace=True to overwrite.")
            fs.delete_dir(path)

        if not isinstance(fs, pa_fs.LocalFileSystem):
            raise NotImplementedError(
                "Full-text search is only supported on the local filesystem"
            )

        if tokenizer_name is None:
            tokenizer_name = "default"
        index = create_index(
            path,
            field_names,
            ordering_fields=ordering_field_names,
            tokenizer_name=tokenizer_name,
        )
        populate_index(
            index,
            self,
            field_names,
            ordering_fields=ordering_field_names,
            writer_heap_size=writer_heap_size,
        )

    @staticmethod
    def infer_tokenizer_configs(tokenizer_name: str) -> dict:
        if tokenizer_name == "default":
            return {
                "base_tokenizer": "simple",
                "language": "English",
                "max_token_length": 40,
                "lower_case": True,
                "stem": False,
                "remove_stop_words": False,
                "ascii_folding": False,
                "ngram_min_length": 3,
                "ngram_max_length": 3,
                "prefix_only": False,
            }
        elif tokenizer_name == "raw":
            return {
                "base_tokenizer": "raw",
                "language": "English",
                "max_token_length": None,
                "lower_case": False,
                "stem": False,
                "remove_stop_words": False,
                "ascii_folding": False,
                "ngram_min_length": 3,
                "ngram_max_length": 3,
                "prefix_only": False,
            }
        elif tokenizer_name == "whitespace":
            return {
                "base_tokenizer": "whitespace",
                "language": "English",
                "max_token_length": None,
                "lower_case": False,
                "stem": False,
                "remove_stop_words": False,
                "ascii_folding": False,
                "ngram_min_length": 3,
                "ngram_max_length": 3,
                "prefix_only": False,
            }

        # or it's with language stemming with pattern like "en_stem"
        if len(tokenizer_name) != 7:
            raise ValueError(f"Invalid tokenizer name {tokenizer_name}")
        lang = tokenizer_name[:2]
        if tokenizer_name[-5:] != "_stem":
            raise ValueError(f"Invalid tokenizer name {tokenizer_name}")
        if lang not in lang_mapping:
            raise ValueError(f"Invalid language code {lang}")
        return {
            "base_tokenizer": "simple",
            "language": lang_mapping[lang],
            "max_token_length": 40,
            "lower_case": True,
            "stem": True,
            "remove_stop_words": False,
            "ascii_folding": False,
            "ngram_min_length": 3,
            "ngram_max_length": 3,
            "prefix_only": False,
        }

    def add(
        self,
        data: DATA,
        mode: AddMode = "append",
        on_bad_vectors: OnBadVectorsType = "error",
        fill_value: float = 0.0,
    ) -> AddResult:
        """Add data to the table.
        If vector columns are missing and the table
        has embedding functions, then the vector columns
        are automatically computed and added.

        Parameters
        ----------
        data: list-of-dict, pd.DataFrame
            The data to insert into the table.
        mode: str
            The mode to use when writing the data. Valid values are
            "append" and "overwrite".
        on_bad_vectors: str, default "error"
            What to do if any of the vectors are not the same size or contains NaNs.
            One of "error", "drop", "fill", "null".
        fill_value: float, default 0.
            The value to use when filling vectors. Only used if on_bad_vectors="fill".

        Returns
        -------
        int
            The number of vectors in the table.
        """
        return LOOP.run(
            self._table.add(
                data, mode=mode, on_bad_vectors=on_bad_vectors, fill_value=fill_value
            )
        )

    def merge(
        self,
        other_table: Union[LanceTable, DATA],
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
        if isinstance(other_table, LanceTable):
            other_table = other_table.to_lance()
        else:
            other_table = _sanitize_data(
                other_table,
                schema,
            )
        self.to_lance().merge(
            other_table, left_on=left_on, right_on=right_on, schema=schema
        )
        self.checkout_latest()

    @cached_property
    def embedding_functions(self) -> Dict[str, EmbeddingFunctionConfig]:
        """
        Get the embedding functions for the table

        Returns
        -------
        funcs: Dict[str, EmbeddingFunctionConfig]
            A mapping of the vector column to the embedding function
            or empty dict if not configured.
        """
        return EmbeddingFunctionRegistry.get_instance().parse_functions(
            self.schema.metadata
        )

    @overload
    def search(  # type: ignore
        self,
        query: Optional[Union[VEC, str, "PIL.Image.Image", Tuple]] = None,
        vector_column_name: Optional[str] = None,
        query_type: Literal["vector"] = "vector",
        ordering_field_name: Optional[str] = None,
        fts_columns: Optional[Union[str, List[str]]] = None,
    ) -> LanceVectorQueryBuilder: ...

    @overload
    def search(
        self,
        query: Optional[Union[VEC, str, "PIL.Image.Image", Tuple]] = None,
        vector_column_name: Optional[str] = None,
        query_type: Literal["fts"] = "fts",
        ordering_field_name: Optional[str] = None,
        fts_columns: Optional[Union[str, List[str]]] = None,
    ) -> LanceFtsQueryBuilder: ...

    @overload
    def search(
        self,
        query: Optional[
            Union[VEC, str, "PIL.Image.Image", Tuple, FullTextQuery]
        ] = None,
        vector_column_name: Optional[str] = None,
        query_type: Literal["hybrid"] = "hybrid",
        ordering_field_name: Optional[str] = None,
        fts_columns: Optional[Union[str, List[str]]] = None,
    ) -> LanceHybridQueryBuilder: ...

    @overload
    def search(
        self,
        query: None = None,
        vector_column_name: Optional[str] = None,
        query_type: QueryType = "auto",
        ordering_field_name: Optional[str] = None,
        fts_columns: Optional[Union[str, List[str]]] = None,
    ) -> LanceEmptyQueryBuilder: ...

    def search(
        self,
        query: Optional[
            Union[VEC, str, "PIL.Image.Image", Tuple, FullTextQuery]
        ] = None,
        vector_column_name: Optional[str] = None,
        query_type: QueryType = "auto",
        ordering_field_name: Optional[str] = None,
        fts_columns: Optional[Union[str, List[str]]] = None,
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
        >>> (table.search(query)
        ...     .where("original_width > 1000", prefilter=True)
        ...     .select(["caption", "original_width", "vector"])
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
        vector_column_name: str, optional
            The name of the vector column to search.

            The vector column needs to be a pyarrow fixed size list type
            *default "vector"*

            - If not specified then the vector column is inferred from
            the table schema

            - If the table has multiple vector columns then the *vector_column_name*
            needs to be specified. Otherwise, an error is raised.
        query_type: str, default "auto"
            "vector", "fts", or "auto"
            If "auto" then the query type is inferred from the query;
            If `query` is a list/np.ndarray then the query type is "vector";
            If `query` is a PIL.Image.Image then either do vector search
            or raise an error if no corresponding embedding function is found.
            If the `query` is a string, then the query type is "vector" if the
            table has embedding functions, else the query type is "fts"
        fts_columns: str or list of str, default None
            The column(s) to search in for full-text search.
            If None then the search is performed on all indexed columns.
            For now, only one column can be searched at a time.

        Returns
        -------
        LanceQueryBuilder
            A query builder object representing the query.
            Once executed, the query returns selected columns, the vector,
            and also the "_distance" column which is the distance between the query
            vector and the returned vector.
        """
        if isinstance(query, FullTextQuery):
            query_type = "fts"
        vector_column_name = infer_vector_column_name(
            schema=self.schema,
            query_type=query_type,
            query=query,
            vector_column_name=vector_column_name,
        )

        return LanceQueryBuilder.create(
            self,
            query,
            query_type,
            vector_column_name=vector_column_name,
            ordering_field_name=ordering_field_name,
            fts_columns=fts_columns or [],
        )

    @classmethod
    def create(
        cls,
        db: LanceDBConnection,
        name: str,
        data: Optional[DATA] = None,
        schema: Optional[pa.Schema] = None,
        mode: CreateMode = "create",
        exist_ok: bool = False,
        on_bad_vectors: OnBadVectorsType = "error",
        fill_value: float = 0.0,
        embedding_functions: Optional[List[EmbeddingFunctionConfig]] = None,
        *,
        storage_options: Optional[Dict[str, str | bool]] = None,
        data_storage_version: Optional[str] = None,
        enable_v2_manifest_paths: Optional[bool] = None,
    ):
        """
        Create a new table.

        Examples
        --------
        >>> import lancedb
        >>> data = [
        ...    {"x": 1, "vector": [1.0, 2]},
        ...    {"x": 2, "vector": [3.0, 4]},
        ...    {"x": 3, "vector": [5.0, 6]}
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
            One of "error", "drop", "fill", "null".
        fill_value: float, default 0.
            The value to use when filling vectors. Only used if on_bad_vectors="fill".
        embedding_functions: list of EmbeddingFunctionModel, default None
            The embedding functions to use when creating the table.
        data_storage_version: optional, str, default "stable"
            Deprecated.  Set `storage_options` when connecting to the database and set
            `new_table_data_storage_version` in the options.
        enable_v2_manifest_paths: optional, bool, default False
            Deprecated.  Set `storage_options` when connecting to the database and set
            `new_table_enable_v2_manifest_paths` in the options.
        """
        self = cls.__new__(cls)
        self._conn = db

        if data_storage_version is not None:
            warnings.warn(
                "setting data_storage_version directly on create_table is deprecated. ",
                "Use database_options instead.",
                DeprecationWarning,
            )
            if storage_options is None:
                storage_options = {}
            storage_options["new_table_data_storage_version"] = data_storage_version
        if enable_v2_manifest_paths is not None:
            warnings.warn(
                "setting enable_v2_manifest_paths directly on create_table is ",
                "deprecated. Use database_options instead.",
                DeprecationWarning,
            )
            if storage_options is None:
                storage_options = {}
            storage_options["new_table_enable_v2_manifest_paths"] = (
                enable_v2_manifest_paths
            )

        self._table = LOOP.run(
            self._conn._conn.create_table(
                name,
                data,
                schema=schema,
                mode=mode,
                exist_ok=exist_ok,
                on_bad_vectors=on_bad_vectors,
                fill_value=fill_value,
                embedding_functions=embedding_functions,
                storage_options=storage_options,
            )
        )
        return self

    def delete(self, where: str) -> DeleteResult:
        return LOOP.run(self._table.delete(where))

    def update(
        self,
        where: Optional[str] = None,
        values: Optional[dict] = None,
        *,
        values_sql: Optional[Dict[str, str]] = None,
    ) -> UpdateResult:
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

        Returns
        -------
        UpdateResult
            - rows_updated: The number of rows that were updated
            - version: The new version number of the table after the update

        Examples
        --------
        >>> import lancedb
        >>> import pandas as pd
        >>> data = pd.DataFrame({"x": [1, 2, 3], "vector": [[1.0, 2], [3, 4], [5, 6]]})
        >>> db = lancedb.connect("./.lancedb")
        >>> table = db.create_table("my_table", data)
        >>> table.to_pandas()
           x      vector
        0  1  [1.0, 2.0]
        1  2  [3.0, 4.0]
        2  3  [5.0, 6.0]
        >>> table.update(where="x = 2", values={"vector": [10.0, 10]})
        UpdateResult(rows_updated=1, version=2)
        >>> table.to_pandas()
           x        vector
        0  1    [1.0, 2.0]
        1  3    [5.0, 6.0]
        2  2  [10.0, 10.0]

        """
        return LOOP.run(self._table.update(values, where=where, updates_sql=values_sql))

    def _execute_query(
        self,
        query: Query,
        *,
        batch_size: Optional[int] = None,
        timeout: Optional[timedelta] = None,
    ) -> pa.RecordBatchReader:
        async_iter = LOOP.run(
            self._table._execute_query(query, batch_size=batch_size, timeout=timeout)
        )

        def iter_sync():
            try:
                while True:
                    yield LOOP.run(async_iter.__anext__())
            except StopAsyncIteration:
                return

        return pa.RecordBatchReader.from_batches(async_iter.schema, iter_sync())

    def _explain_plan(self, query: Query, verbose: Optional[bool] = False) -> str:
        return LOOP.run(self._table._explain_plan(query, verbose))

    def _analyze_plan(self, query: Query) -> str:
        return LOOP.run(self._table._analyze_plan(query))

    def _do_merge(
        self,
        merge: LanceMergeInsertBuilder,
        new_data: DATA,
        on_bad_vectors: OnBadVectorsType,
        fill_value: float,
    ) -> MergeResult:
        return LOOP.run(
            self._table._do_merge(merge, new_data, on_bad_vectors, fill_value)
        )

    @deprecation.deprecated(
        deprecated_in="0.21.0",
        current_version=__version__,
        details="Use `Table.optimize` instead.",
    )
    def cleanup_old_versions(
        self,
        older_than: Optional[timedelta] = None,
        *,
        delete_unverified: bool = False,
    ) -> "CleanupStats":
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

    @deprecation.deprecated(
        deprecated_in="0.21.0",
        current_version=__version__,
        details="Use `Table.optimize` instead.",
    )
    def compact_files(self, *args, **kwargs) -> CompactionStats:
        """
        Run the compaction process on the table.

        This can be run after making several small appends to optimize the table
        for faster reads.

        Arguments are passed onto `lance.dataset.DatasetOptimizer.compact_files`.
         (see Lance documentation for more details) For most cases, the default
        should be fine.
        """
        stats = self.to_lance().optimize.compact_files(*args, **kwargs)
        self.checkout_latest()
        return stats

    def optimize(
        self,
        *,
        cleanup_older_than: Optional[timedelta] = None,
        delete_unverified: bool = False,
        retrain: bool = False,
    ):
        """
        Optimize the on-disk data and indices for better performance.

        Modeled after ``VACUUM`` in PostgreSQL.

        Optimization covers three operations:

         * Compaction: Merges small files into larger ones
         * Prune: Removes old versions of the dataset
         * Index: Optimizes the indices, adding new data to existing indices

        Parameters
        ----------
        cleanup_older_than: timedelta, optional default 7 days
            All files belonging to versions older than this will be removed.  Set
            to 0 days to remove all versions except the latest.  The latest version
            is never removed.
        delete_unverified: bool, default False
            Files leftover from a failed transaction may appear to be part of an
            in-progress operation (e.g. appending new data) and these files will not
            be deleted unless they are at least 7 days old. If delete_unverified is True
            then these files will be deleted regardless of their age.
        retrain: bool, default False
            If True, retrain the vector indices, this would refine the IVF clustering
            and quantization, which may improve the search accuracy. It's faster than
            re-creating the index from scratch, so it's recommended to try this first,
            when the data distribution has changed significantly.

        Experimental API
        ----------------

        The optimization process is undergoing active development and may change.
        Our goal with these changes is to improve the performance of optimization and
        reduce the complexity.

        That being said, it is essential today to run optimize if you want the best
        performance.  It should be stable and safe to use in production, but it our
        hope that the API may be simplified (or not even need to be called) in the
        future.

        The frequency an application shoudl call optimize is based on the frequency of
        data modifications.  If data is frequently added, deleted, or updated then
        optimize should be run frequently.  A good rule of thumb is to run optimize if
        you have added or modified 100,000 or more records or run more than 20 data
        modification operations.
        """
        LOOP.run(
            self._table.optimize(
                cleanup_older_than=cleanup_older_than,
                delete_unverified=delete_unverified,
                retrain=retrain,
            )
        )

    def list_indices(self) -> Iterable[IndexConfig]:
        """
        List all indices that have been created with Self::create_index
        """
        return LOOP.run(self._table.list_indices())

    def index_stats(self, index_name: str) -> Optional[IndexStatistics]:
        """
        Retrieve statistics about an index

        Parameters
        ----------
        index_name: str
            The name of the index to retrieve statistics for

        Returns
        -------
        IndexStatistics or None
            The statistics about the index. Returns None if the index does not exist.
        """
        return LOOP.run(self._table.index_stats(index_name))

    def add_columns(
        self, transforms: Dict[str, str] | pa.field | List[pa.field] | pa.Schema
    ) -> AddColumnsResult:
        return LOOP.run(self._table.add_columns(transforms))

    def alter_columns(
        self, *alterations: Iterable[Dict[str, str]]
    ) -> AlterColumnsResult:
        return LOOP.run(self._table.alter_columns(*alterations))

    def drop_columns(self, columns: Iterable[str]) -> DropColumnsResult:
        return LOOP.run(self._table.drop_columns(columns))

    def uses_v2_manifest_paths(self) -> bool:
        """
        Check if the table is using the new v2 manifest paths.

        Returns
        -------
        bool
            True if the table is using the new v2 manifest paths, False otherwise.
        """
        return LOOP.run(self._table.uses_v2_manifest_paths())

    def migrate_v2_manifest_paths(self):
        """
        Migrate the manifest paths to the new format.

        This will update the manifest to use the new v2 format for paths.

        This function is idempotent, and can be run multiple times without
        changing the state of the object store.

        !!! danger

            This should not be run while other concurrent operations are happening.
            And it should also run until completion before resuming other operations.

        You can use
        [LanceTable.uses_v2_manifest_paths][lancedb.table.LanceTable.uses_v2_manifest_paths]
        to check if the table is already using the new path style.
        """
        LOOP.run(self._table.migrate_v2_manifest_paths())

    def replace_field_metadata(self, field_name: str, new_metadata: Dict[str, str]):
        """
        Replace the metadata of a field in the schema

        Parameters
        ----------
        field_name: str
            The name of the field to replace the metadata for
        new_metadata: dict
            The new metadata to set
        """
        LOOP.run(self._table.replace_field_metadata(field_name, new_metadata))


def _handle_bad_vectors(
    reader: pa.RecordBatchReader,
    on_bad_vectors: Literal["error", "drop", "fill", "null"] = "error",
    fill_value: float = 0.0,
) -> pa.RecordBatchReader:
    vector_columns = []

    for field in reader.schema:
        # They can provide a 'vector' column that isn't yet a FSL
        named_vector_col = (
            (
                pa.types.is_list(field.type)
                or pa.types.is_large_list(field.type)
                or pa.types.is_fixed_size_list(field.type)
            )
            and pa.types.is_floating(field.type.value_type)
            and field.name == VECTOR_COLUMN_NAME
        )
        # TODO: we're making an assumption that fixed size list of 10 or more
        # is a vector column. This is definitely a bit hacky.
        likely_vector_col = (
            pa.types.is_fixed_size_list(field.type)
            and pa.types.is_floating(field.type.value_type)
            and (field.type.list_size >= 10)
        )

        if named_vector_col or likely_vector_col:
            vector_columns.append(field.name)

    def gen():
        for batch in reader:
            for name in vector_columns:
                batch = _handle_bad_vector_column(
                    batch,
                    vector_column_name=name,
                    on_bad_vectors=on_bad_vectors,
                    fill_value=fill_value,
                )
            yield batch

    return pa.RecordBatchReader.from_batches(reader.schema, gen())


def _handle_bad_vector_column(
    data: pa.RecordBatch,
    vector_column_name: str,
    on_bad_vectors: str = "error",
    fill_value: float = 0.0,
) -> pa.RecordBatch:
    """
    Ensure that the vector column exists and has type fixed_size_list(float)

    Parameters
    ----------
    data: pa.Table
        The table to sanitize.
    vector_column_name: str
        The name of the vector column.
    on_bad_vectors: str, default "error"
        What to do if any of the vectors are not the same size or contains NaNs.
        One of "error", "drop", "fill", "null".
    fill_value: float, default 0.0
        The value to use when filling vectors. Only used if on_bad_vectors="fill".
    """
    vec_arr = data[vector_column_name]

    has_nan = has_nan_values(vec_arr)

    if pa.types.is_fixed_size_list(vec_arr.type):
        dim = vec_arr.type.list_size
    else:
        dim = _modal_list_size(vec_arr)
    has_wrong_dim = pc.not_equal(pc.list_value_length(vec_arr), dim)

    has_bad_vectors = pc.any(has_nan).as_py() or pc.any(has_wrong_dim).as_py()

    if has_bad_vectors:
        is_bad = pc.or_(has_nan, has_wrong_dim)
        if on_bad_vectors == "error":
            if pc.any(has_wrong_dim).as_py():
                raise ValueError(
                    f"Vector column '{vector_column_name}' has variable length "
                    "vectors. Set on_bad_vectors='drop' to remove them, "
                    "set on_bad_vectors='fill' and fill_value=<value> to replace them, "
                    "or set on_bad_vectors='null' to replace them with null."
                )
            else:
                raise ValueError(
                    f"Vector column '{vector_column_name}' has NaNs. "
                    "Set on_bad_vectors='drop' to remove them, "
                    "set on_bad_vectors='fill' and fill_value=<value> to replace them, "
                    "or set on_bad_vectors='null' to replace them with null."
                )
        elif on_bad_vectors == "null":
            vec_arr = pc.if_else(
                is_bad,
                pa.scalar(None),
                vec_arr,
            )
        elif on_bad_vectors == "drop":
            data = data.filter(pc.invert(is_bad))
            vec_arr = data[vector_column_name]
        elif on_bad_vectors == "fill":
            if fill_value is None:
                raise ValueError(
                    "`fill_value` must not be None if `on_bad_vectors` is 'fill'"
                )
            vec_arr = pc.if_else(
                is_bad,
                pa.scalar([fill_value] * dim),
                vec_arr,
            )
        else:
            raise ValueError(f"Invalid value for on_bad_vectors: {on_bad_vectors}")

    position = data.column_names.index(vector_column_name)
    return data.set_column(position, vector_column_name, vec_arr)


def has_nan_values(arr: Union[pa.ListArray, pa.ChunkedArray]) -> pa.BooleanArray:
    if isinstance(arr, pa.ChunkedArray):
        values = pa.chunked_array([chunk.flatten() for chunk in arr.chunks])
    else:
        values = arr.flatten()
    if pa.types.is_float16(values.type):
        # is_nan isn't yet implemented for f16, so we cast to f32
        # https://github.com/apache/arrow/issues/45083
        values_has_nan = pc.is_nan(values.cast(pa.float32()))
    else:
        values_has_nan = pc.is_nan(values)
    values_indices = pc.list_parent_indices(arr)
    has_nan_indices = pc.unique(pc.filter(values_indices, values_has_nan))
    indices = pa.array(range(len(arr)), type=pa.uint32())
    return pc.is_in(indices, has_nan_indices)


def _infer_target_schema(
    reader: pa.RecordBatchReader,
) -> Tuple[pa.Schema, pa.RecordBatchReader]:
    schema = reader.schema
    peeked = None

    for i, field in enumerate(schema):
        if (
            field.name == VECTOR_COLUMN_NAME
            and (pa.types.is_list(field.type) or pa.types.is_large_list(field.type))
            and pa.types.is_floating(field.type.value_type)
        ):
            if peeked is None:
                peeked, reader = peek_reader(reader)
            # Use the most common length of the list as the dimensions
            dim = _modal_list_size(peeked.column(i))

            new_field = pa.field(
                VECTOR_COLUMN_NAME,
                pa.list_(pa.float32(), dim),
                nullable=field.nullable,
            )

            schema = schema.set(i, new_field)
        elif (
            field.name == VECTOR_COLUMN_NAME
            and (pa.types.is_list(field.type) or pa.types.is_large_list(field.type))
            and pa.types.is_integer(field.type.value_type)
        ):
            if peeked is None:
                peeked, reader = peek_reader(reader)
            # Use the most common length of the list as the dimensions
            dim = _modal_list_size(peeked.column(i))
            new_field = pa.field(
                VECTOR_COLUMN_NAME,
                pa.list_(pa.uint8(), dim),
                nullable=field.nullable,
            )

            schema = schema.set(i, new_field)

    return schema, reader


def _modal_list_size(arr: Union[pa.ListArray, pa.ChunkedArray]) -> int:
    # Use the most common length of the list as the dimensions
    return pc.mode(pc.list_value_length(arr))[0].as_py()["mode"]


def _validate_schema(schema: pa.Schema):
    """
    Make sure the metadata is valid utf8
    """
    if schema.metadata is not None:
        _validate_metadata(schema.metadata)


def _validate_metadata(metadata: dict):
    """
    Make sure the metadata values are valid utf8 (can be nested)

    Raises ValueError if not valid utf8
    """
    for k, v in metadata.items():
        if isinstance(v, bytes):
            try:
                v.decode("utf8")
            except UnicodeDecodeError:
                raise ValueError(
                    f"Metadata key {k} is not valid utf8. "
                    "Consider base64 encode for generic binary metadata."
                )
        elif isinstance(v, dict):
            _validate_metadata(v)


class AsyncTable:
    """
    An AsyncTable is a collection of Records in a LanceDB Database.

    An AsyncTable can be obtained from the
    [AsyncConnection.create_table][lancedb.AsyncConnection.create_table] and
    [AsyncConnection.open_table][lancedb.AsyncConnection.open_table] methods.

    An AsyncTable object is expected to be long lived and reused for multiple
    operations. AsyncTable objects will cache a certain amount of index data in memory.
    This cache will be freed when the Table is garbage collected.  To eagerly free the
    cache you can call the [close][lancedb.AsyncTable.close] method.  Once the
    AsyncTable is closed, it cannot be used for any further operations.

    An AsyncTable can also be used as a context manager, and will automatically close
    when the context is exited.  Closing a table is optional.  If you do not close the
    table, it will be closed when the AsyncTable object is garbage collected.

    Examples
    --------

    Create using [AsyncConnection.create_table][lancedb.AsyncConnection.create_table]
    (more examples in that method's documentation).

    >>> import lancedb
    >>> async def create_a_table():
    ...     db = await lancedb.connect_async("./.lancedb")
    ...     data = [{"vector": [1.1, 1.2], "b": 2}]
    ...     table = await db.create_table("my_table", data=data)
    ...     print(await table.query().limit(5).to_arrow())
    >>> import asyncio
    >>> asyncio.run(create_a_table())
    pyarrow.Table
    vector: fixed_size_list<item: float>[2]
      child 0, item: float
    b: int64
    ----
    vector: [[[1.1,1.2]]]
    b: [[2]]

    Can append new data with [AsyncTable.add()][lancedb.table.AsyncTable.add].

    >>> async def add_to_table():
    ...     db = await lancedb.connect_async("./.lancedb")
    ...     table = await db.open_table("my_table")
    ...     await table.add([{"vector": [0.5, 1.3], "b": 4}])
    >>> asyncio.run(add_to_table())

    Can query the table with
    [AsyncTable.vector_search][lancedb.table.AsyncTable.vector_search].

    >>> async def search_table_for_vector():
    ...     db = await lancedb.connect_async("./.lancedb")
    ...     table = await db.open_table("my_table")
    ...     results = (
    ...       await table.vector_search([0.4, 0.4]).select(["b", "vector"]).to_pandas()
    ...     )
    ...     print(results)
    >>> asyncio.run(search_table_for_vector())
       b      vector  _distance
    0  4  [0.5, 1.3]       0.82
    1  2  [1.1, 1.2]       1.13

    Search queries are much faster when an index is created. See
    [AsyncTable.create_index][lancedb.table.AsyncTable.create_index].
    """

    def __init__(self, table: LanceDBTable):
        """Create a new AsyncTable object.

        You should not create AsyncTable objects directly.

        Use [AsyncConnection.create_table][lancedb.AsyncConnection.create_table] and
        [AsyncConnection.open_table][lancedb.AsyncConnection.open_table] to obtain
        Table objects."""
        self._inner = table

    def __repr__(self):
        return self._inner.__repr__()

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.close()

    def is_open(self) -> bool:
        """Return True if the table is open."""
        return self._inner.is_open()

    def close(self):
        """Close the table and free any resources associated with it.

        It is safe to call this method multiple times.

        Any attempt to use the table after it has been closed will raise an error."""
        return self._inner.close()

    @property
    def name(self) -> str:
        """The name of the table."""
        return self._inner.name()

    async def schema(self) -> pa.Schema:
        """The [Arrow Schema](https://arrow.apache.org/docs/python/api/datatypes.html#)
        of this Table

        """
        return await self._inner.schema()

    async def embedding_functions(self) -> Dict[str, EmbeddingFunctionConfig]:
        """
        Get the embedding functions for the table

        Returns
        -------
        funcs: Dict[str, EmbeddingFunctionConfig]
            A mapping of the vector column to the embedding function
            or empty dict if not configured.
        """
        schema = await self.schema()
        return EmbeddingFunctionRegistry.get_instance().parse_functions(schema.metadata)

    async def count_rows(self, filter: Optional[str] = None) -> int:
        """
        Count the number of rows in the table.

        Parameters
        ----------
        filter: str, optional
            A SQL where clause to filter the rows to count.
        """
        return await self._inner.count_rows(filter)

    async def head(self, n=5) -> pa.Table:
        """
        Return the first `n` rows of the table.

        Parameters
        ----------
        n: int, default 5
            The number of rows to return.
        """
        return await self.query().limit(n).to_arrow()

    def query(self) -> AsyncQuery:
        """
        Returns an [AsyncQuery][lancedb.query.AsyncQuery] that can be used
        to search the table.

        Use methods on the returned query to control query behavior.  The query
        can be executed with methods like [to_arrow][lancedb.query.AsyncQuery.to_arrow],
        [to_pandas][lancedb.query.AsyncQuery.to_pandas] and more.
        """
        return AsyncQuery(self._inner.query())

    async def to_pandas(self) -> "pd.DataFrame":
        """Return the table as a pandas DataFrame.

        Returns
        -------
        pd.DataFrame
        """
        return (await self.to_arrow()).to_pandas()

    async def to_arrow(self) -> pa.Table:
        """Return the table as a pyarrow Table.

        Returns
        -------
        pa.Table
        """
        return await self.query().to_arrow()

    async def create_index(
        self,
        column: str,
        *,
        replace: Optional[bool] = None,
        config: Optional[
            Union[IvfFlat, IvfPq, HnswPq, HnswSq, BTree, Bitmap, LabelList, FTS]
        ] = None,
        wait_timeout: Optional[timedelta] = None,
    ):
        """Create an index to speed up queries

        Indices can be created on vector columns or scalar columns.
        Indices on vector columns will speed up vector searches.
        Indices on scalar columns will speed up filtering (in both
        vector and non-vector searches)

        Parameters
        ----------
        column: str
            The column to index.
        replace: bool, default True
            Whether to replace the existing index

            If this is false, and another index already exists on the same columns
            and the same name, then an error will be returned.  This is true even if
            that index is out of date.

            The default is True
        config: default None
            For advanced configuration you can specify the type of index you would
            like to create.   You can also specify index-specific parameters when
            creating an index object.
        wait_timeout: timedelta, optional
            The timeout to wait if indexing is asynchronous.
        """
        if config is not None:
            if not isinstance(
                config, (IvfFlat, IvfPq, HnswPq, HnswSq, BTree, Bitmap, LabelList, FTS)
            ):
                raise TypeError(
                    "config must be an instance of IvfPq, HnswPq, HnswSq, BTree,"
                    " Bitmap, LabelList, or FTS"
                )
        try:
            await self._inner.create_index(
                column, index=config, replace=replace, wait_timeout=wait_timeout
            )
        except ValueError as e:
            if "not support the requested language" in str(e):
                supported_langs = ", ".join(lang_mapping.values())
                help_msg = f"Supported languages: {supported_langs}"
                add_note(e, help_msg)
            raise e

    async def drop_index(self, name: str) -> None:
        """
        Drop an index from the table.

        Parameters
        ----------
        name: str
            The name of the index to drop.

        Notes
        -----
        This does not delete the index from disk, it just removes it from the table.
        To delete the index, run [optimize][lancedb.table.AsyncTable.optimize]
        after dropping the index.

        Use [list_indices][lancedb.table.AsyncTable.list_indices] to find the names
        of the indices.
        """
        await self._inner.drop_index(name)

    async def prewarm_index(self, name: str) -> None:
        """
        Prewarm an index in the table.

        Parameters
        ----------
        name: str
            The name of the index to prewarm

        Notes
        -----
        This will load the index into memory.  This may reduce the cold-start time for
        future queries.  If the index does not fit in the cache then this call may be
        wasteful.
        """
        await self._inner.prewarm_index(name)

    async def wait_for_index(
        self, index_names: Iterable[str], timeout: timedelta = timedelta(seconds=300)
    ) -> None:
        """
        Wait for indexing to complete for the given index names.
        This will poll the table until all the indices are fully indexed,
        or raise a timeout exception if the timeout is reached.

        Parameters
        ----------
        index_names: str
            The name of the indices to poll
        timeout: timedelta
            Timeout to wait for asynchronous indexing. The default is 5 minutes.
        """
        await self._inner.wait_for_index(index_names, timeout)

    async def stats(self) -> TableStatistics:
        """
        Retrieve table and fragment statistics.
        """
        return await self._inner.stats()

    async def add(
        self,
        data: DATA,
        *,
        mode: Optional[Literal["append", "overwrite"]] = "append",
        on_bad_vectors: Optional[OnBadVectorsType] = None,
        fill_value: Optional[float] = None,
    ) -> AddResult:
        """Add more data to the [Table](Table).

        Parameters
        ----------
        data: DATA
            The data to insert into the table. Acceptable types are:

            - list-of-dict

            - pandas.DataFrame

            - pyarrow.Table or pyarrow.RecordBatch
        mode: str
            The mode to use when writing the data. Valid values are
            "append" and "overwrite".
        on_bad_vectors: str, default "error"
            What to do if any of the vectors are not the same size or contains NaNs.
            One of "error", "drop", "fill", "null".
        fill_value: float, default 0.
            The value to use when filling vectors. Only used if on_bad_vectors="fill".

        """
        schema = await self.schema()
        if on_bad_vectors is None:
            on_bad_vectors = "error"
        if fill_value is None:
            fill_value = 0.0
        data = _sanitize_data(
            data,
            schema,
            metadata=schema.metadata,
            on_bad_vectors=on_bad_vectors,
            fill_value=fill_value,
            allow_subschema=True,
        )
        if isinstance(data, pa.Table):
            data = data.to_reader()

        return await self._inner.add(data, mode or "append")

    def merge_insert(self, on: Union[str, Iterable[str]]) -> LanceMergeInsertBuilder:
        """
        Returns a [`LanceMergeInsertBuilder`][lancedb.merge.LanceMergeInsertBuilder]
        that can be used to create a "merge insert" operation

        This operation can add rows, update rows, and remove rows all in a single
        transaction. It is a very generic tool that can be used to create
        behaviors like "insert if not exists", "update or insert (i.e. upsert)",
        or even replace a portion of existing data with new data (e.g. replace
        all data where month="january")

        The merge insert operation works by combining new data from a
        **source table** with existing data in a **target table** by using a
        join.  There are three categories of records.

        "Matched" records are records that exist in both the source table and
        the target table. "Not matched" records exist only in the source table
        (e.g. these are new data) "Not matched by source" records exist only
        in the target table (this is old data)

        The builder returned by this method can be used to customize what
        should happen for each category of data.

        Please note that the data may appear to be reordered as part of this
        operation.  This is because updated rows will be deleted from the
        dataset and then reinserted at the end with the new values.

        Parameters
        ----------

        on: Union[str, Iterable[str]]
            A column (or columns) to join on.  This is how records from the
            source table and target table are matched.  Typically this is some
            kind of key or id column.

        Examples
        --------
        >>> import lancedb
        >>> data = pa.table({"a": [2, 1, 3], "b": ["a", "b", "c"]})
        >>> db = lancedb.connect("./.lancedb")
        >>> table = db.create_table("my_table", data)
        >>> new_data = pa.table({"a": [2, 3, 4], "b": ["x", "y", "z"]})
        >>> # Perform a "upsert" operation
        >>> res = table.merge_insert("a")     \\
        ...      .when_matched_update_all()     \\
        ...      .when_not_matched_insert_all() \\
        ...      .execute(new_data)
        >>> res
        MergeResult(version=2, num_updated_rows=2, num_inserted_rows=1, num_deleted_rows=0)
        >>> # The order of new rows is non-deterministic since we use
        >>> # a hash-join as part of this operation and so we sort here
        >>> table.to_arrow().sort_by("a").to_pandas()
           a  b
        0  1  b
        1  2  x
        2  3  y
        3  4  z
        """  # noqa: E501
        on = [on] if isinstance(on, str) else list(iter(on))

        return LanceMergeInsertBuilder(self, on)

    @overload
    async def search(
        self,
        query: Optional[str] = None,
        vector_column_name: Optional[str] = None,
        query_type: Literal["auto"] = ...,
        ordering_field_name: Optional[str] = None,
        fts_columns: Optional[Union[str, List[str]]] = None,
    ) -> Union[AsyncHybridQuery, AsyncFTSQuery, AsyncVectorQuery]: ...

    @overload
    async def search(
        self,
        query: Optional[str] = None,
        vector_column_name: Optional[str] = None,
        query_type: Literal["hybrid"] = ...,
        ordering_field_name: Optional[str] = None,
        fts_columns: Optional[Union[str, List[str]]] = None,
    ) -> AsyncHybridQuery: ...

    @overload
    async def search(
        self,
        query: Optional[Union[VEC, "PIL.Image.Image", Tuple]] = None,
        vector_column_name: Optional[str] = None,
        query_type: Literal["auto"] = ...,
        ordering_field_name: Optional[str] = None,
        fts_columns: Optional[Union[str, List[str]]] = None,
    ) -> AsyncVectorQuery: ...

    @overload
    async def search(
        self,
        query: Optional[str] = None,
        vector_column_name: Optional[str] = None,
        query_type: Literal["fts"] = ...,
        ordering_field_name: Optional[str] = None,
        fts_columns: Optional[Union[str, List[str]]] = None,
    ) -> AsyncFTSQuery: ...

    @overload
    async def search(
        self,
        query: Optional[
            Union[VEC, str, "PIL.Image.Image", Tuple, FullTextQuery]
        ] = None,
        vector_column_name: Optional[str] = None,
        query_type: Literal["vector"] = ...,
        ordering_field_name: Optional[str] = None,
        fts_columns: Optional[Union[str, List[str]]] = None,
    ) -> AsyncVectorQuery: ...

    async def search(
        self,
        query: Optional[
            Union[VEC, str, "PIL.Image.Image", Tuple, FullTextQuery]
        ] = None,
        vector_column_name: Optional[str] = None,
        query_type: QueryType = "auto",
        ordering_field_name: Optional[str] = None,
        fts_columns: Optional[Union[str, List[str]]] = None,
    ) -> Union[AsyncHybridQuery, AsyncFTSQuery, AsyncVectorQuery]:
        """Create a search query to find the nearest neighbors
        of the given query vector. We currently support [vector search][search]
        and [full-text search][experimental-full-text-search].

        All query options are defined in [AsyncQuery][lancedb.query.AsyncQuery].

        Parameters
        ----------
        query: list/np.ndarray/str/PIL.Image.Image, default None
            The targetted vector to search for.

            - *default None*.
            Acceptable types are: list, np.ndarray, PIL.Image.Image

            - If None then the select/where/limit clauses are applied to filter
            the table
        vector_column_name: str, optional
            The name of the vector column to search.

            The vector column needs to be a pyarrow fixed size list type

            - If not specified then the vector column is inferred from
            the table schema

            - If the table has multiple vector columns then the *vector_column_name*
            needs to be specified. Otherwise, an error is raised.
        query_type: str
            *default "auto"*.
            Acceptable types are: "vector", "fts", "hybrid", or "auto"

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
        """

        def is_embedding(query):
            return isinstance(query, (list, np.ndarray, pa.Array, pa.ChunkedArray))

        async def get_embedding_func(
            vector_column_name: Optional[str],
            query_type: QueryType,
            query: Optional[Union[VEC, str, "PIL.Image.Image", Tuple, FullTextQuery]],
        ) -> Tuple[str, EmbeddingFunctionConfig]:
            if isinstance(query, FullTextQuery):
                query_type = "fts"
            schema = await self.schema()
            vector_column_name = infer_vector_column_name(
                schema=schema,
                query_type=query_type,
                query=query,
                vector_column_name=vector_column_name,
            )
            funcs = EmbeddingFunctionRegistry.get_instance().parse_functions(
                schema.metadata
            )
            func = funcs.get(vector_column_name)
            if func is None:
                error = ValueError(
                    f"Column '{vector_column_name}' has no registered "
                    "embedding function."
                )
                if len(funcs) > 0:
                    add_note(
                        error,
                        "Embedding functions are registered for columns: "
                        f"{list(funcs.keys())}",
                    )
                else:
                    add_note(
                        error, "No embedding functions are registered for any columns."
                    )
                raise error
            return vector_column_name, func

        async def make_embedding(embedding, query):
            if embedding is not None:
                loop = asyncio.get_running_loop()
                # This function is likely to block, since it either calls an expensive
                # function or makes an HTTP request to an embeddings REST API.
                return (
                    await loop.run_in_executor(
                        None,
                        embedding.function.compute_query_embeddings_with_retry,
                        query,
                    )
                )[0]
            else:
                return None

        if query_type == "auto":
            # Infer the query type.
            if is_embedding(query):
                vector_query = query
                query_type = "vector"
            elif isinstance(query, FullTextQuery):
                query_type = "fts"
            elif isinstance(query, str):
                try:
                    (
                        indices,
                        (vector_column_name, embedding_conf),
                    ) = await asyncio.gather(
                        self.list_indices(),
                        get_embedding_func(vector_column_name, "auto", query),
                    )
                except ValueError as e:
                    if "Column" in str(
                        e
                    ) and "has no registered embedding function" in str(e):
                        # If the column has no registered embedding function,
                        # then it's an FTS query.
                        query_type = "fts"
                    else:
                        raise e
                else:
                    if embedding_conf is not None:
                        vector_query = await make_embedding(embedding_conf, query)
                        if any(
                            i.columns[0] == embedding_conf.source_column
                            and i.index_type == "FTS"
                            for i in indices
                        ):
                            query_type = "hybrid"
                        else:
                            query_type = "vector"
                    else:
                        query_type = "fts"
            else:
                # it's an image or something else embeddable.
                query_type = "vector"
        elif query_type == "vector":
            if is_embedding(query):
                vector_query = query
            else:
                vector_column_name, embedding_conf = await get_embedding_func(
                    vector_column_name, query_type, query
                )
                vector_query = await make_embedding(embedding_conf, query)
        elif query_type == "hybrid":
            if is_embedding(query):
                raise ValueError("Hybrid search requires a text query")
            else:
                vector_column_name, embedding_conf = await get_embedding_func(
                    vector_column_name, query_type, query
                )
                vector_query = await make_embedding(embedding_conf, query)

        if query_type == "vector":
            builder = self.query().nearest_to(vector_query)
            if vector_column_name:
                builder = builder.column(vector_column_name)
            return builder
        elif query_type == "fts":
            return self.query().nearest_to_text(query, columns=fts_columns)
        elif query_type == "hybrid":
            builder = self.query().nearest_to(vector_query)
            if vector_column_name:
                builder = builder.column(vector_column_name)
            return builder.nearest_to_text(query, columns=fts_columns)
        else:
            raise ValueError(f"Unknown query type: '{query_type}'")

    def vector_search(
        self,
        query_vector: Union[VEC, Tuple],
    ) -> AsyncVectorQuery:
        """
        Search the table with a given query vector.
        This is a convenience method for preparing a vector query and
        is the same thing as calling `nearestTo` on the builder returned
        by `query`.  Seer [nearest_to][lancedb.query.AsyncQuery.nearest_to] for more
        details.
        """
        return self.query().nearest_to(query_vector)

    def _sync_query_to_async(
        self, query: Query
    ) -> AsyncHybridQuery | AsyncFTSQuery | AsyncVectorQuery | AsyncQuery:
        async_query = self.query()
        if query.limit is not None:
            async_query = async_query.limit(query.limit)
        if query.offset is not None:
            async_query = async_query.offset(query.offset)
        if query.columns:
            async_query = async_query.select(query.columns)
        if query.filter:
            async_query = async_query.where(query.filter)
        if query.fast_search:
            async_query = async_query.fast_search()
        if query.with_row_id:
            async_query = async_query.with_row_id()

        if query.vector:
            async_query = async_query.nearest_to(query.vector).distance_range(
                query.lower_bound, query.upper_bound
            )
            if query.distance_type is not None:
                async_query = async_query.distance_type(query.distance_type)
            if query.minimum_nprobes is not None and query.maximum_nprobes is not None:
                # Set both to the minimum first to avoid min > max error.
                async_query = async_query.nprobes(
                    query.minimum_nprobes
                ).maximum_nprobes(query.maximum_nprobes)
            elif query.minimum_nprobes is not None:
                async_query = async_query.minimum_nprobes(query.minimum_nprobes)
            elif query.maximum_nprobes is not None:
                async_query = async_query.maximum_nprobes(query.maximum_nprobes)
            if query.refine_factor is not None:
                async_query = async_query.refine_factor(query.refine_factor)
            if query.vector_column:
                async_query = async_query.column(query.vector_column)
            if query.ef:
                async_query = async_query.ef(query.ef)
            if query.bypass_vector_index:
                async_query = async_query.bypass_vector_index()

        if query.postfilter:
            async_query = async_query.postfilter()

        if query.full_text_query:
            async_query = async_query.nearest_to_text(
                query.full_text_query.query, query.full_text_query.columns
            )

        return async_query

    async def _execute_query(
        self,
        query: Query,
        *,
        batch_size: Optional[int] = None,
        timeout: Optional[timedelta] = None,
    ) -> pa.RecordBatchReader:
        # The sync table calls into this method, so we need to map the
        # query to the async version of the query and run that here. This is only
        # used for that code path right now.

        async_query = self._sync_query_to_async(query)

        return await async_query.to_batches(
            max_batch_length=batch_size, timeout=timeout
        )

    async def _explain_plan(self, query: Query, verbose: Optional[bool]) -> str:
        # This method is used by the sync table
        async_query = self._sync_query_to_async(query)
        return await async_query.explain_plan(verbose)

    async def _analyze_plan(self, query: Query) -> str:
        # This method is used by the sync table
        async_query = self._sync_query_to_async(query)
        return await async_query.analyze_plan()

    async def _do_merge(
        self,
        merge: LanceMergeInsertBuilder,
        new_data: DATA,
        on_bad_vectors: OnBadVectorsType,
        fill_value: float,
    ) -> MergeResult:
        schema = await self.schema()
        if on_bad_vectors is None:
            on_bad_vectors = "error"
        if fill_value is None:
            fill_value = 0.0
        data = _sanitize_data(
            new_data,
            schema,
            metadata=schema.metadata,
            on_bad_vectors=on_bad_vectors,
            fill_value=fill_value,
            allow_subschema=True,
        )
        if isinstance(data, pa.Table):
            data = pa.RecordBatchReader.from_batches(data.schema, data.to_batches())
        return await self._inner.execute_merge_insert(
            data,
            dict(
                on=merge._on,
                when_matched_update_all=merge._when_matched_update_all,
                when_matched_update_all_condition=merge._when_matched_update_all_condition,
                when_not_matched_insert_all=merge._when_not_matched_insert_all,
                when_not_matched_by_source_delete=merge._when_not_matched_by_source_delete,
                when_not_matched_by_source_condition=merge._when_not_matched_by_source_condition,
                timeout=merge._timeout,
            ),
        )

    async def delete(self, where: str) -> DeleteResult:
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
        ...    {"x": 1, "vector": [1.0, 2]},
        ...    {"x": 2, "vector": [3.0, 4]},
        ...    {"x": 3, "vector": [5.0, 6]}
        ... ]
        >>> db = lancedb.connect("./.lancedb")
        >>> table = db.create_table("my_table", data)
        >>> table.to_pandas()
           x      vector
        0  1  [1.0, 2.0]
        1  2  [3.0, 4.0]
        2  3  [5.0, 6.0]
        >>> table.delete("x = 2")
        DeleteResult(version=2)
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
        DeleteResult(version=3)
        >>> table.to_pandas()
           x      vector
        0  3  [5.0, 6.0]
        """
        return await self._inner.delete(where)

    async def update(
        self,
        updates: Optional[Dict[str, Any]] = None,
        *,
        where: Optional[str] = None,
        updates_sql: Optional[Dict[str, str]] = None,
    ) -> UpdateResult:
        """
        This can be used to update zero to all rows in the table.

        If a filter is provided with `where` then only rows matching the
        filter will be updated.  Otherwise all rows will be updated.

        Parameters
        ----------
        updates: dict, optional
            The updates to apply.  The keys should be the name of the column to
            update.  The values should be the new values to assign.  This is
            required unless updates_sql is supplied.
        where: str, optional
            An SQL filter that controls which rows are updated. For example, 'x = 2'
            or 'x IN (1, 2, 3)'.  Only rows that satisfy this filter will be udpated.
        updates_sql: dict, optional
            The updates to apply, expressed as SQL expression strings.  The keys should
            be column names. The values should be SQL expressions.  These can be SQL
            literals (e.g. "7" or "'foo'") or they can be expressions based on the
            previous value of the row (e.g. "x + 1" to increment the x column by 1)

        Returns
        -------
        UpdateResult
            An object containing:
            - rows_updated: The number of rows that were updated
            - version: The new version number of the table after the update

        Examples
        --------
        >>> import asyncio
        >>> import lancedb
        >>> import pandas as pd
        >>> async def demo_update():
        ...     data = pd.DataFrame({"x": [1, 2], "vector": [[1, 2], [3, 4]]})
        ...     db = await lancedb.connect_async("./.lancedb")
        ...     table = await db.create_table("my_table", data)
        ...     # x is [1, 2], vector is [[1, 2], [3, 4]]
        ...     await table.update({"vector": [10, 10]}, where="x = 2")
        ...     # x is [1, 2], vector is [[1, 2], [10, 10]]
        ...     await table.update(updates_sql={"x": "x + 1"})
        ...     # x is [2, 3], vector is [[1, 2], [10, 10]]
        >>> asyncio.run(demo_update())
        """
        if updates is not None and updates_sql is not None:
            raise ValueError("Only one of updates or updates_sql can be provided")
        if updates is None and updates_sql is None:
            raise ValueError("Either updates or updates_sql must be provided")

        if updates is not None:
            updates_sql = {k: value_to_sql(v) for k, v in updates.items()}

        return await self._inner.update(updates_sql, where)

    async def add_columns(
        self, transforms: dict[str, str] | pa.field | List[pa.field] | pa.Schema
    ) -> AddColumnsResult:
        """
        Add new columns with defined values.

        Parameters
        ----------
        transforms: Dict[str, str]
            A map of column name to a SQL expression to use to calculate the
            value of the new column. These expressions will be evaluated for
            each row in the table, and can reference existing columns.
            Alternatively, you can pass a pyarrow field or schema to add
            new columns with NULLs.

        Returns
        -------
        AddColumnsResult
            version: the new version number of the table after adding columns.

        """
        if isinstance(transforms, pa.Field):
            transforms = [transforms]
        if isinstance(transforms, list) and all(
            {isinstance(f, pa.Field) for f in transforms}
        ):
            transforms = pa.schema(transforms)
        if isinstance(transforms, pa.Schema):
            return await self._inner.add_columns_with_schema(transforms)
        else:
            return await self._inner.add_columns(list(transforms.items()))

    async def alter_columns(
        self, *alterations: Iterable[dict[str, Any]]
    ) -> AlterColumnsResult:
        """
        Alter column names and nullability.

        alterations : Iterable[Dict[str, Any]]
            A sequence of dictionaries, each with the following keys:
            - "path": str
                The column path to alter. For a top-level column, this is the name.
                For a nested column, this is the dot-separated path, e.g. "a.b.c".
            - "rename": str, optional
                The new name of the column. If not specified, the column name is
                not changed.
            - "data_type": pyarrow.DataType, optional
               The new data type of the column. Existing values will be casted
               to this type. If not specified, the column data type is not changed.
            - "nullable": bool, optional
                Whether the column should be nullable. If not specified, the column
                nullability is not changed. Only non-nullable columns can be changed
                to nullable. Currently, you cannot change a nullable column to
                non-nullable.

        Returns
        -------
        AlterColumnsResult
            version: the new version number of the table after the alteration.
        """
        return await self._inner.alter_columns(alterations)

    async def drop_columns(self, columns: Iterable[str]):
        """
        Drop columns from the table.

        Parameters
        ----------
        columns : Iterable[str]
            The names of the columns to drop.
        """
        return await self._inner.drop_columns(columns)

    async def version(self) -> int:
        """
        Retrieve the version of the table

        LanceDb supports versioning.  Every operation that modifies the table increases
        version.  As long as a version hasn't been deleted you can `[Self::checkout]`
        that version to view the data at that point.  In addition, you can
        `[Self::restore]` the version to replace the current table with a previous
        version.
        """
        return await self._inner.version()

    async def list_versions(self):
        """
        List all versions of the table
        """
        versions = await self._inner.list_versions()
        for v in versions:
            ts_nanos = v["timestamp"]
            v["timestamp"] = datetime.fromtimestamp(ts_nanos // 1e9) + timedelta(
                microseconds=(ts_nanos % 1e9) // 1e3
            )

        return versions

    async def checkout(self, version: int | str):
        """
        Checks out a specific version of the Table

        Any read operation on the table will now access the data at the checked out
        version. As a consequence, calling this method will disable any read consistency
        interval that was previously set.

        This is a read-only operation that turns the table into a sort of "view"
        or "detached head".  Other table instances will not be affected.  To make the
        change permanent you can use the `[Self::restore]` method.

        Any operation that modifies the table will fail while the table is in a checked
        out state.

        Parameters
        ----------
        version: int | str,
            The version to check out. A version number (`int`) or a tag
            (`str`) can be provided.

        To return the table to a normal state use `[Self::checkout_latest]`
        """
        try:
            await self._inner.checkout(version)
        except RuntimeError as e:
            if "not found" in str(e):
                raise ValueError(
                    f"Version {version} no longer exists. Was it cleaned up?"
                )
            else:
                raise

    async def checkout_latest(self):
        """
        Ensures the table is pointing at the latest version

        This can be used to manually update a table when the read_consistency_interval
        is None
        It can also be used to undo a `[Self::checkout]` operation
        """
        await self._inner.checkout_latest()

    async def restore(self, version: Optional[int | str] = None):
        """
        Restore the table to the currently checked out version

        This operation will fail if checkout has not been called previously

        This operation will overwrite the latest version of the table with a
        previous version.  Any changes made since the checked out version will
        no longer be visible.

        Once the operation concludes the table will no longer be in a checked
        out state and the read_consistency_interval, if any, will apply.
        """
        await self._inner.restore(version)

    @property
    def tags(self) -> AsyncTags:
        """Tag management for the dataset.

        Similar to Git, tags are a way to add metadata to a specific version of the
        dataset.

        .. warning::

            Tagged versions are exempted from the
            :py:meth:`optimize(cleanup_older_than)` process.

            To remove a version that has been tagged, you must first
            :py:meth:`~Tags.delete` the associated tag.

        """
        return AsyncTags(self._inner)

    async def optimize(
        self,
        *,
        cleanup_older_than: Optional[timedelta] = None,
        delete_unverified: bool = False,
        retrain=False,
    ) -> OptimizeStats:
        """
        Optimize the on-disk data and indices for better performance.

        Modeled after ``VACUUM`` in PostgreSQL.

        Optimization covers three operations:

         * Compaction: Merges small files into larger ones
         * Prune: Removes old versions of the dataset
         * Index: Optimizes the indices, adding new data to existing indices

        Parameters
        ----------
        cleanup_older_than: timedelta, optional default 7 days
            All files belonging to versions older than this will be removed.  Set
            to 0 days to remove all versions except the latest.  The latest version
            is never removed.
        delete_unverified: bool, default False
            Files leftover from a failed transaction may appear to be part of an
            in-progress operation (e.g. appending new data) and these files will not
            be deleted unless they are at least 7 days old. If delete_unverified is True
            then these files will be deleted regardless of their age.
        retrain: bool, default False
            If True, retrain the vector indices, this would refine the IVF clustering
            and quantization, which may improve the search accuracy. It's faster than
            re-creating the index from scratch, so it's recommended to try this first,
            when the data distribution has changed significantly.

        Experimental API
        ----------------

        The optimization process is undergoing active development and may change.
        Our goal with these changes is to improve the performance of optimization and
        reduce the complexity.

        That being said, it is essential today to run optimize if you want the best
        performance.  It should be stable and safe to use in production, but it our
        hope that the API may be simplified (or not even need to be called) in the
        future.

        The frequency an application shoudl call optimize is based on the frequency of
        data modifications.  If data is frequently added, deleted, or updated then
        optimize should be run frequently.  A good rule of thumb is to run optimize if
        you have added or modified 100,000 or more records or run more than 20 data
        modification operations.
        """
        cleanup_since_ms: Optional[int] = None
        if cleanup_older_than is not None:
            cleanup_since_ms = round(cleanup_older_than.total_seconds() * 1000)
        return await self._inner.optimize(
            cleanup_since_ms=cleanup_since_ms,
            delete_unverified=delete_unverified,
            retrain=retrain,
        )

    async def list_indices(self) -> Iterable[IndexConfig]:
        """
        List all indices that have been created with Self::create_index
        """
        return await self._inner.list_indices()

    async def index_stats(self, index_name: str) -> Optional[IndexStatistics]:
        """
        Retrieve statistics about an index

        Parameters
        ----------
        index_name: str
            The name of the index to retrieve statistics for

        Returns
        -------
        IndexStatistics or None
            The statistics about the index. Returns None if the index does not exist.
        """
        stats = await self._inner.index_stats(index_name)
        if stats is None:
            return None
        else:
            return IndexStatistics(**stats)

    async def uses_v2_manifest_paths(self) -> bool:
        """
        Check if the table is using the new v2 manifest paths.

        Returns
        -------
        bool
            True if the table is using the new v2 manifest paths, False otherwise.
        """
        return await self._inner.uses_v2_manifest_paths()

    async def migrate_manifest_paths_v2(self):
        """
        Migrate the manifest paths to the new format.

        This will update the manifest to use the new v2 format for paths.

        This function is idempotent, and can be run multiple times without
        changing the state of the object store.

        !!! danger

            This should not be run while other concurrent operations are happening.
            And it should also run until completion before resuming other operations.

        You can use
        [AsyncTable.uses_v2_manifest_paths][lancedb.table.AsyncTable.uses_v2_manifest_paths]
        to check if the table is already using the new path style.
        """
        await self._inner.migrate_manifest_paths_v2()

    async def replace_field_metadata(
        self, field_name: str, new_metadata: dict[str, str]
    ):
        """
        Replace the metadata of a field in the schema

        Parameters
        ----------
        field_name: str
            The name of the field to replace the metadata for
        new_metadata: dict
            The new metadata to set
        """
        await self._inner.replace_field_metadata(field_name, new_metadata)


@dataclass
class IndexStatistics:
    """
    Statistics about an index.

    Attributes
    ----------
    num_indexed_rows: int
        The number of rows that are covered by this index.
    num_unindexed_rows: int
        The number of rows that are not covered by this index.
    index_type: str
        The type of index that was created.
    distance_type: Optional[str]
        The distance type used by the index.
    num_indices: Optional[int]
        The number of parts the index is split into.
    loss: Optional[float]
        The KMeans loss for the index, for only vector indices.
    """

    num_indexed_rows: int
    num_unindexed_rows: int
    index_type: Literal[
        "IVF_PQ", "IVF_HNSW_PQ", "IVF_HNSW_SQ", "FTS", "BTREE", "BITMAP", "LABEL_LIST"
    ]
    distance_type: Optional[Literal["l2", "cosine", "dot"]] = None
    num_indices: Optional[int] = None
    loss: Optional[float] = None

    # This exists for backwards compatibility with an older API, which returned
    # a dictionary instead of a class.
    def __getitem__(self, key):
        return getattr(self, key)


@dataclass
class TableStatistics:
    """
    Statistics about a table and fragments.

    Attributes
    ----------
    total_bytes: int
        The total number of bytes in the table.
    num_rows: int
        The total number of rows in the table.
    num_indices: int
        The total number of indices in the table.
    fragment_stats: FragmentStatistics
        Statistics about fragments in the table.
    """

    total_bytes: int
    num_rows: int
    num_indices: int
    fragment_stats: FragmentStatistics


@dataclass
class FragmentStatistics:
    """
    Statistics about fragments.

    Attributes
    ----------
    num_fragments: int
        The total number of fragments in the table.
    num_small_fragments: int
        The total number of small fragments in the table.
        Small fragments have low row counts and may need to be compacted.
    lengths: FragmentSummaryStats
        Statistics about the number of rows in the table fragments.
    """

    num_fragments: int
    num_small_fragments: int
    lengths: FragmentSummaryStats


@dataclass
class FragmentSummaryStats:
    """
    Statistics about fragments sizes

    Attributes
    ----------
    min: int
        The number of rows in the fragment with the fewest rows.
    max: int
        The number of rows in the fragment with the most rows.
    mean: int
        The mean number of rows in the fragments.
    p25: int
        The 25th percentile of number of rows in the fragments.
    p50: int
        The 50th percentile of number of rows in the fragments.
    p75: int
        The 75th percentile of number of rows in the fragments.
    p99: int
        The 99th percentile of number of rows in the fragments.
    """

    min: int
    max: int
    mean: int
    p25: int
    p50: int
    p75: int
    p99: int


class Tags:
    """
    Table tag manager.
    """

    def __init__(self, table):
        self._table = table

    def list(self) -> Dict[str, Tag]:
        """
        List all table tags.

        Returns
        -------
        dict[str, Tag]
            A dictionary mapping tag names to version numbers.
        """
        return LOOP.run(self._table.tags.list())

    def get_version(self, tag: str) -> int:
        """
        Get the version of a tag.

        Parameters
        ----------
        tag: str,
            The name of the tag to get the version for.
        """
        return LOOP.run(self._table.tags.get_version(tag))

    def create(self, tag: str, version: int) -> None:
        """
        Create a tag for a given table version.

        Parameters
        ----------
        tag: str,
            The name of the tag to create. This name must be unique among all tag
            names for the table.
        version: int,
            The table version to tag.
        """
        LOOP.run(self._table.tags.create(tag, version))

    def delete(self, tag: str) -> None:
        """
        Delete tag from the table.

        Parameters
        ----------
        tag: str,
            The name of the tag to delete.
        """
        LOOP.run(self._table.tags.delete(tag))

    def update(self, tag: str, version: int) -> None:
        """
        Update tag to a new version.

        Parameters
        ----------
        tag: str,
            The name of the tag to update.
        version: int,
            The new table version to tag.
        """
        LOOP.run(self._table.tags.update(tag, version))


class AsyncTags:
    """
    Async table tag manager.
    """

    def __init__(self, table):
        self._table = table

    async def list(self) -> Dict[str, Tag]:
        """
        List all table tags.

        Returns
        -------
        dict[str, Tag]
            A dictionary mapping tag names to version numbers.
        """
        return await self._table.tags.list()

    async def get_version(self, tag: str) -> int:
        """
        Get the version of a tag.

        Parameters
        ----------
        tag: str,
            The name of the tag to get the version for.
        """
        return await self._table.tags.get_version(tag)

    async def create(self, tag: str, version: int) -> None:
        """
        Create a tag for a given table version.

        Parameters
        ----------
        tag: str,
            The name of the tag to create. This name must be unique among all tag
            names for the table.
        version: int,
            The table version to tag.
        """
        await self._table.tags.create(tag, version)

    async def delete(self, tag: str) -> None:
        """
        Delete tag from the table.

        Parameters
        ----------
        tag: str,
            The name of the tag to delete.
        """
        await self._table.tags.delete(tag)

    async def update(self, tag: str, version: int) -> None:
        """
        Update tag to a new version.

        Parameters
        ----------
        tag: str,
            The name of the tag to update.
        version: int,
            The new table version to tag.
        """
        await self._table.tags.update(tag, version)
