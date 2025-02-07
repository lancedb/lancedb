# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

from __future__ import annotations

import inspect
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

import lance
from lancedb.arrow import peek_reader
from lancedb.background_loop import LOOP
from .dependencies import _check_for_pandas
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.fs as pa_fs
from lance import LanceDataset
from lance.dependencies import _check_for_hugging_face

from .common import DATA, VEC, VECTOR_COLUMN_NAME
from .embeddings import EmbeddingFunctionConfig, EmbeddingFunctionRegistry
from .index import BTree, IvfFlat, IvfPq, Bitmap, LabelList, HnswPq, HnswSq, FTS
from .merge import LanceMergeInsertBuilder
from .pydantic import LanceModel, model_to_dict
from .query import (
    AsyncQuery,
    AsyncVectorQuery,
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
    safe_import_pandas,
    safe_import_polars,
    value_to_sql,
)
from .index import lang_mapping


if TYPE_CHECKING:
    from ._lancedb import Table as LanceDBTable, OptimizeStats, CompactionStats
    from .db import LanceDBConnection
    from .index import IndexConfig
    from lance.dataset import CleanupStats, ReaderLike
    import pandas
    import PIL

pd = safe_import_pandas()
pl = safe_import_polars()

QueryType = Literal["vector", "fts", "hybrid", "auto"]


def _into_pyarrow_reader(data) -> pa.RecordBatchReader:
    if _check_for_hugging_face(data):
        # Huggingface datasets
        from lance.dependencies import datasets

        if isinstance(data, datasets.Dataset):
            schema = data.features.arrow_schema
            return pa.RecordBatchReader.from_batches(schema, data.data.to_batches())
        elif isinstance(data, datasets.dataset_dict.DatasetDict):
            schema = _schema_from_hf(data, schema)
            return pa.RecordBatchReader.from_batches(
                schema, _to_batches_with_split(data)
            )
    if isinstance(data, LanceModel):
        raise ValueError("Cannot add a single LanceModel to a table. Use a list.")

    if isinstance(data, dict):
        raise ValueError("Cannot add a single dictionary to a table. Use a list.")

    if isinstance(data, list):
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
    elif isinstance(data, LanceDataset):
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
            "Please check "
            "https://lancedb.github.io/lancedb/python/python/ "
            "to see supported types."
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
    on_bad_vectors: Literal["error", "drop", "fill", "null"] = "error",
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
    reader = _into_pyarrow_reader(data)

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

    fields = []
    for field in reader.schema:
        target_field = target_schema.field(field.name)
        if target_field is None:
            raise ValueError(f"Field {field.name} not found in target schema")
        fields.append(target_field)
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
    on_bad_vectors: str = "error",
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


def _schema_from_hf(data, schema):
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
        metric="L2",
        num_partitions=256,
        num_sub_vectors=96,
        vector_column_name: str = VECTOR_COLUMN_NAME,
        replace: bool = True,
        accelerator: Optional[str] = None,
        index_cache_size: Optional[int] = None,
        *,
        index_type: Literal[
            "IVF_FLAT", "IVF_PQ", "IVF_HNSW_SQ", "IVF_HNSW_PQ"
        ] = "IVF_PQ",
        num_bits: int = 8,
        max_iterations: int = 50,
        sample_rate: int = 256,
        m: int = 20,
        ef_construction: int = 300,
    ):
        """Create an index on the table.

        Parameters
        ----------
        metric: str, default "L2"
            The distance metric to use when creating the index.
            Valid values are "L2", "cosine", "dot", or "hamming".
            L2 is euclidean distance.
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

    @abstractmethod
    def create_scalar_index(
        self,
        column: str,
        *,
        replace: bool = True,
        index_type: Literal["BTREE", "BITMAP", "LABEL_LIST"] = "BTREE",
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
        use_tantivy: bool = True,
        tokenizer_name: Optional[str] = None,
        with_position: bool = True,
        # tokenizer configs:
        base_tokenizer: Literal["simple", "raw", "whitespace"] = "simple",
        language: str = "English",
        max_token_length: Optional[int] = 40,
        lower_case: bool = True,
        stem: bool = False,
        remove_stop_words: bool = False,
        ascii_folding: bool = False,
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
        use_tantivy: bool, default True
            If True, use the legacy full-text search implementation based on tantivy.
            If False, use the new full-text search implementation based on lance-index.
        with_position: bool, default True
            Only available with use_tantivy=False
            If False, do not store the positions of the terms in the text.
            This can reduce the size of the index and improve indexing speed.
            But it will raise an exception for phrase queries.
        base_tokenizer : str, default "simple"
            The base tokenizer to use for tokenization. Options are:
            - "simple": Splits text by whitespace and punctuation.
            - "whitespace": Split text by whitespace, but not punctuation.
            - "raw": No tokenization. The entire text is treated as a single token.
        language : str, default "English"
            The language to use for tokenization.
        max_token_length : int, default 40
            The maximum token length to index. Tokens longer than this length will be
            ignored.
        lower_case : bool, default True
            Whether to convert the token to lower case. This makes queries
            case-insensitive.
        stem : bool, default False
            Whether to stem the token. Stemming reduces words to their root form.
            For example, in English "running" and "runs" would both be reduced to "run".
        remove_stop_words : bool, default False
            Whether to remove stop words. Stop words are common words that are often
            removed from text before indexing. For example, in English "the" and "and".
        ascii_folding : bool, default False
            Whether to fold ASCII characters. This converts accented characters to
            their ASCII equivalent. For example, "café" would be converted to "cafe".
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
        >>> table.merge_insert("a")             \\
        ...      .when_matched_update_all()     \\
        ...      .when_not_matched_insert_all() \\
        ...      .execute(new_data)
        >>> # The order of new rows is non-deterministic since we use
        >>> # a hash-join as part of this operation and so we sort here
        >>> table.to_arrow().sort_by("a").to_pandas()
           a  b
        0  1  b
        1  2  x
        2  3  y
        3  4  z
        """
        on = [on] if isinstance(on, str) else list(iter(on))

        return LanceMergeInsertBuilder(self, on)

    @abstractmethod
    def search(
        self,
        query: Optional[Union[VEC, str, "PIL.Image.Image", Tuple]] = None,
        vector_column_name: Optional[str] = None,
        query_type: QueryType = "auto",
        ordering_field_name: Optional[str] = None,
        fts_columns: Optional[Union[str, List[str]]] = None,
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
        self, query: Query, batch_size: Optional[int] = None
    ) -> pa.RecordBatchReader: ...

    @abstractmethod
    def _do_merge(
        self,
        merge: LanceMergeInsertBuilder,
        new_data: DATA,
        on_bad_vectors: str,
        fill_value: float,
    ): ...

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
        >>> data = pd.DataFrame({"x": [1, 2, 3], "vector": [[1.0, 2], [3, 4], [5, 6]]})
        >>> db = lancedb.connect("./.lancedb")
        >>> table = db.create_table("my_table", data)
        >>> table.to_pandas()
           x      vector
        0  1  [1.0, 2.0]
        1  2  [3.0, 4.0]
        2  3  [5.0, 6.0]
        >>> table.update(where="x = 2", values={"vector": [10.0, 10]})
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

    @abstractmethod
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
    def add_columns(self, transforms: Dict[str, str]):
        """
        Add new columns with defined values.

        Parameters
        ----------
        transforms: Dict[str, str]
            A map of column name to a SQL expression to use to calculate the
            value of the new column. These expressions will be evaluated for
            each row in the table, and can reference existing columns.
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
        """

    @abstractmethod
    def drop_columns(self, columns: Iterable[str]):
        """
        Drop columns from the table.

        Parameters
        ----------
        columns : Iterable[str]
            The names of the columns to drop.
        """

    @abstractmethod
    def checkout(self, version: int):
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

    def to_lance(self, **kwargs) -> LanceDataset:
        """Return the LanceDataset backing this table."""
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

    def checkout(self, version: int):
        """Checkout a version of the table. This is an in-place operation.

        This allows viewing previous versions of the table. If you wish to
        keep writing to the dataset starting from an old version, then use
        the `restore` function.

        Calling this method will set the table into time-travel mode. If you
        wish to return to standard mode, call `checkout_latest`.

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
        LOOP.run(self._table.checkout(version))

    def checkout_latest(self):
        """Checkout the latest version of the table. This is an in-place operation.

        The table will be set back into standard mode, and will track the latest
        version of the table.
        """
        LOOP.run(self._table.checkout_latest())

    def restore(self, version: Optional[int] = None):
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
        1
        >>> table.to_pandas()
               vector    type
        0  [1.1, 0.9]  vector
        >>> table.add([{"vector": [0.5, 0.2], "type": "vector"}])
        >>> table.version
        2
        >>> table.restore(1)
        >>> table.to_pandas()
               vector    type
        0  [1.1, 0.9]  vector
        >>> len(table.list_versions())
        3
        """
        if version is not None:
            LOOP.run(self._table.checkout(version))
        LOOP.run(self._table.restore())

    def count_rows(self, filter: Optional[str] = None) -> int:
        return LOOP.run(self._table.count_rows(filter))

    def __len__(self) -> int:
        return self.count_rows()

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
        metric="L2",
        num_partitions=None,
        num_sub_vectors=None,
        vector_column_name=VECTOR_COLUMN_NAME,
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
        return LOOP.run(self._table.drop_index(name))

    def create_scalar_index(
        self,
        column: str,
        *,
        replace: bool = True,
        index_type: Literal["BTREE", "BITMAP", "LABEL_LIST"] = "BTREE",
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
        use_tantivy: bool = True,
        tokenizer_name: Optional[str] = None,
        with_position: bool = True,
        # tokenizer configs:
        base_tokenizer: str = "simple",
        language: str = "English",
        max_token_length: Optional[int] = 40,
        lower_case: bool = True,
        stem: bool = False,
        remove_stop_words: bool = False,
        ascii_folding: bool = False,
    ):
        if not use_tantivy:
            if not isinstance(field_names, str):
                raise ValueError("field_names must be a string when use_tantivy=False")

            if tokenizer_name is None:
                tokenizer_configs = {
                    "base_tokenizer": base_tokenizer,
                    "language": language,
                    "max_token_length": max_token_length,
                    "lower_case": lower_case,
                    "stem": stem,
                    "remove_stop_words": remove_stop_words,
                    "ascii_folding": ascii_folding,
                }
            else:
                tokenizer_configs = self.infer_tokenizer_configs(tokenizer_name)

            config = FTS(
                with_position=with_position,
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
        }

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
        LOOP.run(
            self._table.add(
                data, mode=mode, on_bad_vectors=on_bad_vectors, fill_value=fill_value
            )
        )

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
        query: Optional[Union[VEC, str, "PIL.Image.Image", Tuple]] = None,
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
        query: Optional[Union[VEC, str, "PIL.Image.Image", Tuple]] = None,
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
            fts_columns=fts_columns,
        )

    @classmethod
    def create(
        cls,
        db: LanceDBConnection,
        name: str,
        data: Optional[DATA] = None,
        schema: Optional[pa.Schema] = None,
        mode: Literal["create", "overwrite"] = "create",
        exist_ok: bool = False,
        on_bad_vectors: str = "error",
        fill_value: float = 0.0,
        embedding_functions: Optional[List[EmbeddingFunctionConfig]] = None,
        *,
        storage_options: Optional[Dict[str, str]] = None,
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

    def delete(self, where: str):
        LOOP.run(self._table.delete(where))

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
        >>> data = pd.DataFrame({"x": [1, 2, 3], "vector": [[1.0, 2], [3, 4], [5, 6]]})
        >>> db = lancedb.connect("./.lancedb")
        >>> table = db.create_table("my_table", data)
        >>> table.to_pandas()
           x      vector
        0  1  [1.0, 2.0]
        1  2  [3.0, 4.0]
        2  3  [5.0, 6.0]
        >>> table.update(where="x = 2", values={"vector": [10.0, 10]})
        >>> table.to_pandas()
           x        vector
        0  1    [1.0, 2.0]
        1  3    [5.0, 6.0]
        2  2  [10.0, 10.0]

        """
        LOOP.run(self._table.update(values, where=where, updates_sql=values_sql))

    def _execute_query(
        self, query: Query, batch_size: Optional[int] = None
    ) -> pa.RecordBatchReader:
        return LOOP.run(self._table._execute_query(query, batch_size))

    def _do_merge(
        self,
        merge: LanceMergeInsertBuilder,
        new_data: DATA,
        on_bad_vectors: str,
        fill_value: float,
    ):
        LOOP.run(self._table._do_merge(merge, new_data, on_bad_vectors, fill_value))

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

    def add_columns(self, transforms: Dict[str, str]):
        LOOP.run(self._table.add_columns(transforms))

    def alter_columns(self, *alterations: Iterable[Dict[str, str]]):
        LOOP.run(self._table.alter_columns(*alterations))

    def drop_columns(self, columns: Iterable[str]):
        LOOP.run(self._table.drop_columns(columns))

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
        """Return True if the table is closed."""
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
            await self._inner.create_index(column, index=config, replace=replace)
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

    async def add(
        self,
        data: DATA,
        *,
        mode: Optional[Literal["append", "overwrite"]] = "append",
        on_bad_vectors: Optional[str] = None,
        fill_value: Optional[float] = None,
    ):
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

        await self._inner.add(data, mode or "append")

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
        >>> table.merge_insert("a")             \\
        ...      .when_matched_update_all()     \\
        ...      .when_not_matched_insert_all() \\
        ...      .execute(new_data)
        >>> # The order of new rows is non-deterministic since we use
        >>> # a hash-join as part of this operation and so we sort here
        >>> table.to_arrow().sort_by("a").to_pandas()
           a  b
        0  1  b
        1  2  x
        2  3  y
        3  4  z
        """
        on = [on] if isinstance(on, str) else list(iter(on))

        return LanceMergeInsertBuilder(self, on)

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

    async def _execute_query(
        self, query: Query, batch_size: Optional[int] = None
    ) -> pa.RecordBatchReader:
        # The sync remote table calls into this method, so we need to map the
        # query to the async version of the query and run that here. This is only
        # used for that code path right now.
        async_query = self.query().limit(query.k)
        if query.offset > 0:
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
            # we need the schema to get the vector column type
            # to determine whether the vectors is batch queries or not
            async_query = (
                async_query.nearest_to(query.vector)
                .distance_type(query.metric)
                .nprobes(query.nprobes)
                .distance_range(query.lower_bound, query.upper_bound)
            )
            if query.refine_factor:
                async_query = async_query.refine_factor(query.refine_factor)
            if query.vector_column:
                async_query = async_query.column(query.vector_column)
            if query.ef:
                async_query = async_query.ef(query.ef)
            if not query.use_index:
                async_query = async_query.bypass_vector_index()

        if not query.prefilter:
            async_query = async_query.postfilter()

        if isinstance(query.full_text_query, str):
            async_query = async_query.nearest_to_text(query.full_text_query)
        elif isinstance(query.full_text_query, dict):
            fts_query = query.full_text_query["query"]
            fts_columns = query.full_text_query.get("columns", []) or []
            async_query = async_query.nearest_to_text(fts_query, columns=fts_columns)

        table = await async_query.to_arrow()
        return table.to_reader()

    async def _do_merge(
        self,
        merge: LanceMergeInsertBuilder,
        new_data: DATA,
        on_bad_vectors: str,
        fill_value: float,
    ):
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
        await self._inner.execute_merge_insert(
            data,
            dict(
                on=merge._on,
                when_matched_update_all=merge._when_matched_update_all,
                when_matched_update_all_condition=merge._when_matched_update_all_condition,
                when_not_matched_insert_all=merge._when_not_matched_insert_all,
                when_not_matched_by_source_delete=merge._when_not_matched_by_source_delete,
                when_not_matched_by_source_condition=merge._when_not_matched_by_source_condition,
            ),
        )

    async def delete(self, where: str):
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
        return await self._inner.delete(where)

    async def update(
        self,
        updates: Optional[Dict[str, Any]] = None,
        *,
        where: Optional[str] = None,
        updates_sql: Optional[Dict[str, str]] = None,
    ):
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

    async def add_columns(self, transforms: dict[str, str]):
        """
        Add new columns with defined values.

        Parameters
        ----------
        transforms: Dict[str, str]
            A map of column name to a SQL expression to use to calculate the
            value of the new column. These expressions will be evaluated for
            each row in the table, and can reference existing columns.
        """
        await self._inner.add_columns(list(transforms.items()))

    async def alter_columns(self, *alterations: Iterable[dict[str, Any]]):
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
        """
        await self._inner.alter_columns(alterations)

    async def drop_columns(self, columns: Iterable[str]):
        """
        Drop columns from the table.

        Parameters
        ----------
        columns : Iterable[str]
            The names of the columns to drop.
        """
        await self._inner.drop_columns(columns)

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

    async def checkout(self, version: int):
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

    async def restore(self):
        """
        Restore the table to the currently checked out version

        This operation will fail if checkout has not been called previously

        This operation will overwrite the latest version of the table with a
        previous version.  Any changes made since the checked out version will
        no longer be visible.

        Once the operation concludes the table will no longer be in a checked
        out state and the read_consistency_interval, if any, will apply.
        """
        await self._inner.restore()

    async def optimize(
        self,
        *,
        cleanup_older_than: Optional[timedelta] = None,
        delete_unverified: bool = False,
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
            cleanup_since_ms=cleanup_since_ms, delete_unverified=delete_unverified
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
    """

    num_indexed_rows: int
    num_unindexed_rows: int
    index_type: Literal[
        "IVF_PQ", "IVF_HNSW_PQ", "IVF_HNSW_SQ", "FTS", "BTREE", "BITMAP", "LABEL_LIST"
    ]
    distance_type: Optional[Literal["l2", "cosine", "dot"]] = None
    num_indices: Optional[int] = None

    # This exists for backwards compatibility with an older API, which returned
    # a dictionary instead of a class.
    def __getitem__(self, key):
        return getattr(self, key)
