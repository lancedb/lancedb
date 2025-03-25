# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

from __future__ import annotations

from abc import ABC, abstractmethod
from concurrent.futures import ThreadPoolExecutor
from typing import (
    TYPE_CHECKING,
    Dict,
    List,
    Literal,
    Optional,
    Tuple,
    Type,
    Union,
    Any,
)

import asyncio
import deprecation
import numpy as np
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.fs as pa_fs
import pydantic

from . import __version__
from .arrow import AsyncRecordBatchReader
from .dependencies import pandas as pd
from .rerankers.base import Reranker
from .rerankers.rrf import RRFReranker
from .rerankers.util import check_reranker_result
from .util import flatten_columns

from typing_extensions import Annotated

if TYPE_CHECKING:
    import sys
    import PIL
    import polars as pl

    from ._lancedb import Query as LanceQuery
    from ._lancedb import FTSQuery as LanceFTSQuery
    from ._lancedb import HybridQuery as LanceHybridQuery
    from ._lancedb import VectorQuery as LanceVectorQuery
    from ._lancedb import PyQueryRequest
    from .common import VEC
    from .pydantic import LanceModel
    from .table import Table

    if sys.version_info >= (3, 11):
        from typing import Self
    else:
        from typing_extensions import Self


# Pydantic validation function for vector queries
def ensure_vector_query(
    val: Any,
) -> Union[List[float], List[List[float]], pa.Array, List[pa.Array]]:
    if isinstance(val, list):
        if len(val) == 0:
            return ValueError("Vector query must be a non-empty list")
        sample = val[0]
    else:
        if isinstance(val, float):
            raise ValueError(
                "Vector query must be a list of floats or a list of lists of floats"
            )
        sample = val
    if isinstance(sample, pa.Array):
        # val is array or list of array
        return val
    if isinstance(sample, list):
        if len(sample) == 0:
            return ValueError("Vector query must be a non-empty list")
        if isinstance(sample[0], float):
            # val is list of list of floats
            return val
    if isinstance(sample, float):
        # val is a list of floats
        return val


class FullTextSearchQuery(pydantic.BaseModel):
    """A LanceDB Full Text Search Query

    Attributes
    ----------
    columns: List[str]
        The columns to search

        If None, then the table should select the column automatically.
    query: str
        The query to search for
    limit: Optional[int] = None
        The limit on the number of results to return
    wand_factor: Optional[float] = None
        The wand factor to use for the search
    """

    columns: Optional[List[str]] = None
    query: str
    limit: Optional[int] = None
    wand_factor: Optional[float] = None


class Query(pydantic.BaseModel):
    """A LanceDB Query

    Queries are constructed by the `Table.search` method.  This class is a
    python representation of the query.  Normally you will not need to interact
    with this class directly.  You can build up a query and execute it using
    collection methods such as `to_batches()`, `to_arrow()`, `to_pandas()`,
    etc.

    However, you can use the `to_query()` method to get the underlying query object.
    This can be useful for serializing a query or using it in a different context.

    Attributes
    ----------
    filter : Optional[str]
        sql filter to refine the query with
    limit : Optional[int]
        The limit on the number of results to return.  If this is a vector or FTS query,
        then this is required.  If this is a plain SQL query, then this is optional.
    offset: Optional[int]
        The offset to start fetching results from

        This is ignored for vector / FTS search (will be None).
    columns : Optional[Union[List[str], Dict[str, str]]]
        which columns to return in the results

        This can be a list of column names or a dictionary.  If it is a dictionary,
        then the keys are the column names and the values are sql expressions to
        use to calculate the result.

        If this is None then all columns are returned.  This can be expensive.
    with_row_id : Optional[bool]
        if True then include the row id in the results
    vector : Optional[Union[List[float], List[List[float]], pa.Array, List[pa.Array]]]
        the vector to search for, if this a vector search or hybrid search.  It will
        be None for full text search and plain SQL filtering.
    vector_column : Optional[str]
        the name of the vector column to use for vector search

        If this is None then a default vector column will be used.
    distance_type : Optional[str]
        the distance type to use for vector search

        This can be l2 (default), cosine and dot.  See [metric definitions][search] for
        more details.

        If this is not a vector search this will be None.
    postfilter : bool
        if True then apply the filter after vector / FTS search.  This is ignored for
        plain SQL filtering.
    nprobes : Optional[int]
        The number of IVF partitions to search.  If this is None then a default
        number of partitions will be used.

        - A higher number makes search more accurate but also slower.

        - See discussion in [Querying an ANN Index][querying-an-ann-index] for
          tuning advice.

        Will be None if this is not a vector search.
    refine_factor : Optional[int]
        Refine the results by reading extra elements and re-ranking them in memory.

        - A higher number makes search more accurate but also slower.

        - See discussion in [Querying an ANN Index][querying-an-ann-index] for
          tuning advice.

        Will be None if this is not a vector search.
    lower_bound : Optional[float]
        The lower bound for distance search

        Only results with a distance greater than or equal to this value
        will be returned.

        This will only be set on vector search.
    upper_bound : Optional[float]
        The upper bound for distance search

        Only results with a distance less than or equal to this value
        will be returned.

        This will only be set on vector search.
    ef : Optional[int]
        The size of the nearest neighbor list maintained during HNSW search

        This will only be set on vector search.
    full_text_query : Optional[Union[str, dict]]
        The full text search query

        This can be a string or a dictionary.  A dictionary will be used to search
        multiple columns.  The keys are the column names and the values are the
        search queries.

        This will only be set on FTS or hybrid queries.
    fast_search: Optional[bool]
        Skip a flat search of unindexed data. This will improve
        search performance but search results will not include unindexed data.

        The default is False
    """

    # The name of the vector column to use for vector search.
    vector_column: Optional[str] = None

    # vector to search for
    #
    # Note: today this will be floats on the sync path and pa.Array on the async
    # path though in the future we should unify this to pa.Array everywhere
    vector: Annotated[
        Optional[Union[List[float], List[List[float]], pa.Array, List[pa.Array]]],
        ensure_vector_query,
    ] = None

    # sql filter to refine the query with
    filter: Optional[str] = None

    # if True then apply the filter after vector search
    postfilter: Optional[bool] = None

    # full text search query
    full_text_query: Optional[FullTextSearchQuery] = None

    # top k results to return
    limit: Optional[int] = None

    # distance type to use for vector search
    distance_type: Optional[str] = None

    # which columns to return in the results
    columns: Optional[Union[List[str], Dict[str, str]]] = None

    # number of IVF partitions to search
    nprobes: Optional[int] = None

    # lower bound for distance search
    lower_bound: Optional[float] = None

    # upper bound for distance search
    upper_bound: Optional[float] = None

    # multiplier for the number of results to inspect for reranking
    refine_factor: Optional[int] = None

    # if true, include the row id in the results
    with_row_id: Optional[bool] = None

    # offset to start fetching results from
    offset: Optional[int] = None

    # if true, will only search the indexed data
    fast_search: Optional[bool] = None

    # size of the nearest neighbor list maintained during HNSW search
    ef: Optional[int] = None

    # Bypass the vector index and use a brute force search
    bypass_vector_index: Optional[bool] = None

    @classmethod
    def from_inner(cls, req: PyQueryRequest) -> Self:
        query = cls()
        query.limit = req.limit
        query.offset = req.offset
        query.filter = req.filter
        query.full_text_query = req.full_text_search
        query.columns = req.select
        query.with_row_id = req.with_row_id
        query.vector_column = req.column
        query.vector = req.query_vector
        query.distance_type = req.distance_type
        query.nprobes = req.nprobes
        query.lower_bound = req.lower_bound
        query.upper_bound = req.upper_bound
        query.ef = req.ef
        query.refine_factor = req.refine_factor
        query.bypass_vector_index = req.bypass_vector_index
        query.postfilter = req.postfilter
        if req.full_text_search is not None:
            query.full_text_query = FullTextSearchQuery(
                columns=req.full_text_search.columns,
                query=req.full_text_search.query,
                limit=req.full_text_search.limit,
                wand_factor=req.full_text_search.wand_factor,
            )
        return query

    class Config:
        # This tells pydantic to allow custom types (needed for the `vector` query since
        # pa.Array wouln't be allowed otherwise)
        arbitrary_types_allowed = True


class LanceQueryBuilder(ABC):
    """An abstract query builder. Subclasses are defined for vector search,
    full text search, hybrid, and plain SQL filtering.
    """

    @classmethod
    def create(
        cls,
        table: "Table",
        query: Optional[Union[np.ndarray, str, "PIL.Image.Image", Tuple]],
        query_type: str,
        vector_column_name: str,
        ordering_field_name: Optional[str] = None,
        fts_columns: Optional[Union[str, List[str]]] = None,
        fast_search: bool = None,
    ) -> Self:
        """
        Create a query builder based on the given query and query type.

        Parameters
        ----------
        table: Table
            The table to query.
        query: Optional[Union[np.ndarray, str, "PIL.Image.Image", Tuple]]
            The query to use. If None, an empty query builder is returned
            which performs simple SQL filtering.
        query_type: str
            The type of query to perform. One of "vector", "fts", "hybrid", or "auto".
            If "auto", the query type is inferred based on the query.
        vector_column_name: str
            The name of the vector column to use for vector search.
        fast_search: bool
            Skip flat search of unindexed data.
        """
        # Check hybrid search first as it supports empty query pattern
        if query_type == "hybrid":
            # hybrid fts and vector query
            return LanceHybridQueryBuilder(
                table, query, vector_column_name, fts_columns=fts_columns
            )

        if query is None:
            return LanceEmptyQueryBuilder(table)

        # remember the string query for reranking purpose
        str_query = query if isinstance(query, str) else None

        # convert "auto" query_type to "vector", "fts"
        # or "hybrid" and convert the query to vector if needed
        query, query_type = cls._resolve_query(
            table, query, query_type, vector_column_name
        )

        if query_type == "hybrid":
            return LanceHybridQueryBuilder(
                table, query, vector_column_name, fts_columns=fts_columns
            )

        if isinstance(query, str):
            # fts
            return LanceFtsQueryBuilder(
                table,
                query,
                ordering_field_name=ordering_field_name,
                fts_columns=fts_columns,
            )

        if isinstance(query, list):
            query = np.array(query, dtype=np.float32)
        elif isinstance(query, np.ndarray):
            query = query.astype(np.float32)
        else:
            raise TypeError(f"Unsupported query type: {type(query)}")

        return LanceVectorQueryBuilder(
            table, query, vector_column_name, str_query, fast_search
        )

    @classmethod
    def _resolve_query(cls, table, query, query_type, vector_column_name):
        # If query_type is fts, then query must be a string.
        # otherwise raise TypeError
        if query_type == "fts":
            if not isinstance(query, str):
                raise TypeError(f"'fts' queries must be a string: {type(query)}")
            return query, query_type
        elif query_type == "vector":
            query = cls._query_to_vector(table, query, vector_column_name)
            return query, query_type
        elif query_type == "auto":
            if isinstance(query, (list, np.ndarray)):
                return query, "vector"
            else:
                conf = table.embedding_functions.get(vector_column_name)
                if conf is not None:
                    query = conf.function.compute_query_embeddings_with_retry(query)[0]
                    return query, "vector"
                else:
                    return query, "fts"
        else:
            raise ValueError(
                f"Invalid query_type, must be 'vector', 'fts', or 'auto': {query_type}"
            )

    @classmethod
    def _query_to_vector(cls, table, query, vector_column_name):
        if isinstance(query, (list, np.ndarray)):
            return query
        conf = table.embedding_functions.get(vector_column_name)
        if conf is not None:
            return conf.function.compute_query_embeddings_with_retry(query)[0]
        else:
            msg = f"No embedding function for {vector_column_name}"
            raise ValueError(msg)

    def __init__(self, table: "Table"):
        self._table = table
        self._limit = None
        self._offset = None
        self._columns = None
        self._where = None
        self._postfilter = None
        self._with_row_id = None
        self._vector = None
        self._text = None
        self._ef = None
        self._bypass_vector_index = None

    @deprecation.deprecated(
        deprecated_in="0.3.1",
        removed_in="0.4.0",
        current_version=__version__,
        details="Use to_pandas() instead",
    )
    def to_df(self) -> "pd.DataFrame":
        """
        *Deprecated alias for `to_pandas()`. Please use `to_pandas()` instead.*

        Execute the query and return the results as a pandas DataFrame.
        In addition to the selected columns, LanceDB also returns a vector
        and also the "_distance" column which is the distance between the query
        vector and the returned vector.
        """
        return self.to_pandas()

    def to_pandas(self, flatten: Optional[Union[int, bool]] = None) -> "pd.DataFrame":
        """
        Execute the query and return the results as a pandas DataFrame.
        In addition to the selected columns, LanceDB also returns a vector
        and also the "_distance" column which is the distance between the query
        vector and the returned vector.

        Parameters
        ----------
        flatten: Optional[Union[int, bool]]
            If flatten is True, flatten all nested columns.
            If flatten is an integer, flatten the nested columns up to the
            specified depth.
            If unspecified, do not flatten the nested columns.
        """
        tbl = flatten_columns(self.to_arrow(), flatten)
        return tbl.to_pandas()

    @abstractmethod
    def to_arrow(self) -> pa.Table:
        """
        Execute the query and return the results as an
        [Apache Arrow Table](https://arrow.apache.org/docs/python/generated/pyarrow.Table.html#pyarrow.Table).

        In addition to the selected columns, LanceDB also returns a vector
        and also the "_distance" column which is the distance between the query
        vector and the returned vectors.
        """
        raise NotImplementedError

    @abstractmethod
    def to_batches(self, /, batch_size: Optional[int] = None) -> pa.RecordBatchReader:
        """
        Execute the query and return the results as a pyarrow
        [RecordBatchReader](https://arrow.apache.org/docs/python/generated/pyarrow.RecordBatchReader.html)
        """
        raise NotImplementedError

    def to_list(self) -> List[dict]:
        """
        Execute the query and return the results as a list of dictionaries.

        Each list entry is a dictionary with the selected column names as keys,
        or all table columns if `select` is not called. The vector and the "_distance"
        fields are returned whether or not they're explicitly selected.
        """
        return self.to_arrow().to_pylist()

    def to_pydantic(self, model: Type[LanceModel]) -> List[LanceModel]:
        """Return the table as a list of pydantic models.

        Parameters
        ----------
        model: Type[LanceModel]
            The pydantic model to use.

        Returns
        -------
        List[LanceModel]
        """
        return [
            model(**{k: v for k, v in row.items() if k in model.field_names()})
            for row in self.to_arrow().to_pylist()
        ]

    def to_polars(self) -> "pl.DataFrame":
        """
        Execute the query and return the results as a Polars DataFrame.
        In addition to the selected columns, LanceDB also returns a vector
        and also the "_distance" column which is the distance between the query
        vector and the returned vector.
        """
        import polars as pl

        return pl.from_arrow(self.to_arrow())

    def limit(self, limit: Union[int, None]) -> Self:
        """Set the maximum number of results to return.

        Parameters
        ----------
        limit: int
            The maximum number of results to return.
            The default query limit is 10 results.
            For ANN/KNN queries, you must specify a limit.
            For plain searches, all records are returned if limit not set.
            *WARNING* if you have a large dataset, setting
            the limit to a large number, e.g. the table size,
            can potentially result in reading a
            large amount of data into memory and cause
            out of memory issues.

        Returns
        -------
        LanceQueryBuilder
            The LanceQueryBuilder object.
        """
        if limit is None or limit <= 0:
            if isinstance(self, LanceVectorQueryBuilder):
                raise ValueError("Limit is required for ANN/KNN queries")
            else:
                self._limit = None
        else:
            self._limit = limit
        return self

    def offset(self, offset: int) -> Self:
        """Set the offset for the results.

        Parameters
        ----------
        offset: int
            The offset to start fetching results from.

        Returns
        -------
        LanceQueryBuilder
            The LanceQueryBuilder object.
        """
        if offset is None or offset <= 0:
            self._offset = 0
        else:
            self._offset = offset
        return self

    def select(self, columns: Union[list[str], dict[str, str]]) -> Self:
        """Set the columns to return.

        Parameters
        ----------
        columns: list of str, or dict of str to str default None
            List of column names to be fetched.
            Or a dictionary of column names to SQL expressions.
            All columns are fetched if None or unspecified.

        Returns
        -------
        LanceQueryBuilder
            The LanceQueryBuilder object.
        """
        if isinstance(columns, list) or isinstance(columns, dict):
            self._columns = columns
        else:
            raise ValueError("columns must be a list or a dictionary")
        return self

    def where(self, where: str, prefilter: bool = True) -> Self:
        """Set the where clause.

        Parameters
        ----------
        where: str
            The where clause which is a valid SQL where clause. See
            `Lance filter pushdown <https://lancedb.github.io/lance/read_and_write.html#filter-push-down>`_
            for valid SQL expressions.
        prefilter: bool, default True
            If True, apply the filter before vector search, otherwise the
            filter is applied on the result of vector search.
            This feature is **EXPERIMENTAL** and may be removed and modified
            without warning in the future.

        Returns
        -------
        LanceQueryBuilder
            The LanceQueryBuilder object.
        """
        self._where = where
        self._postfilter = not prefilter
        return self

    def with_row_id(self, with_row_id: bool) -> Self:
        """Set whether to return row ids.

        Parameters
        ----------
        with_row_id: bool
            If True, return _rowid column in the results.

        Returns
        -------
        LanceQueryBuilder
            The LanceQueryBuilder object.
        """
        self._with_row_id = with_row_id
        return self

    def explain_plan(self, verbose: Optional[bool] = False) -> str:
        """Return the execution plan for this query.

        Examples
        --------
        >>> import lancedb
        >>> db = lancedb.connect("./.lancedb")
        >>> table = db.create_table("my_table", [{"vector": [99.0, 99]}])
        >>> query = [100, 100]
        >>> plan = table.search(query).explain_plan(True)
        >>> print(plan) # doctest: +ELLIPSIS, +NORMALIZE_WHITESPACE
        ProjectionExec: expr=[vector@0 as vector, _distance@2 as _distance]
        GlobalLimitExec: skip=0, fetch=10
          FilterExec: _distance@2 IS NOT NULL
            SortExec: TopK(fetch=10), expr=[_distance@2 ASC NULLS LAST], preserve_partitioning=[false]
              KNNVectorDistance: metric=l2
                LanceScan: uri=..., projection=[vector], row_id=true, row_addr=false, ordered=false

        Parameters
        ----------
        verbose : bool, default False
            Use a verbose output format.

        Returns
        -------
        plan : str
        """  # noqa: E501
        return self._table._explain_plan(self.to_query_object())

    def vector(self, vector: Union[np.ndarray, list]) -> Self:
        """Set the vector to search for.

        Parameters
        ----------
        vector: np.ndarray or list
            The vector to search for.

        Returns
        -------
        LanceQueryBuilder
            The LanceQueryBuilder object.
        """
        raise NotImplementedError

    def text(self, text: str) -> Self:
        """Set the text to search for.

        Parameters
        ----------
        text: str
            The text to search for.

        Returns
        -------
        LanceQueryBuilder
            The LanceQueryBuilder object.
        """
        raise NotImplementedError

    @abstractmethod
    def rerank(self, reranker: Reranker) -> Self:
        """Rerank the results using the specified reranker.

        Parameters
        ----------
        reranker: Reranker
            The reranker to use.

        Returns
        -------

        The LanceQueryBuilder object.
        """
        raise NotImplementedError

    @abstractmethod
    def to_query_object(self) -> Query:
        """Return a serializable representation of the query

        Returns
        -------
        Query
            The serializable representation of the query
        """
        raise NotImplementedError


class LanceVectorQueryBuilder(LanceQueryBuilder):
    """
    Examples
    --------
    >>> import lancedb
    >>> data = [{"vector": [1.1, 1.2], "b": 2},
    ...         {"vector": [0.5, 1.3], "b": 4},
    ...         {"vector": [0.4, 0.4], "b": 6},
    ...         {"vector": [0.4, 0.4], "b": 10}]
    >>> db = lancedb.connect("./.lancedb")
    >>> table = db.create_table("my_table", data=data)
    >>> (table.search([0.4, 0.4])
    ...       .distance_type("cosine")
    ...       .where("b < 10")
    ...       .select(["b", "vector"])
    ...       .limit(2)
    ...       .to_pandas())
       b      vector  _distance
    0  6  [0.4, 0.4]   0.000000
    1  2  [1.1, 1.2]   0.000944
    """

    def __init__(
        self,
        table: "Table",
        query: Union[np.ndarray, list, "PIL.Image.Image"],
        vector_column: str,
        str_query: Optional[str] = None,
        fast_search: bool = None,
    ):
        super().__init__(table)
        self._query = query
        self._distance_type = None
        self._nprobes = None
        self._lower_bound = None
        self._upper_bound = None
        self._refine_factor = None
        self._vector_column = vector_column
        self._postfilter = None
        self._reranker = None
        self._str_query = str_query
        self._fast_search = fast_search

    def metric(self, metric: Literal["l2", "cosine", "dot"]) -> LanceVectorQueryBuilder:
        """Set the distance metric to use.

        This is an alias for distance_type() and may be deprecated in the future.
        Please use distance_type() instead.

        Parameters
        ----------
        metric: "l2" or "cosine" or "dot"
            The distance metric to use. By default "l2" is used.

        Returns
        -------
        LanceVectorQueryBuilder
            The LanceQueryBuilder object.
        """
        return self.distance_type(metric)

    def distance_type(
        self, distance_type: Literal["l2", "cosine", "dot"]
    ) -> "LanceVectorQueryBuilder":
        """Set the distance metric to use.

        When performing a vector search we try and find the "nearest" vectors according
        to some kind of distance metric. This parameter controls which distance metric
        to use.

        Note: if there is a vector index then the distance type used MUST match the
        distance type used to train the vector index. If this is not done then the
        results will be invalid.

        Parameters
        ----------
        distance_type: "l2" or "cosine" or "dot"
            The distance metric to use. By default "l2" is used.

        Returns
        -------
        LanceVectorQueryBuilder
            The LanceQueryBuilder object.
        """
        self._distance_type = distance_type.lower()
        return self

    def nprobes(self, nprobes: int) -> LanceVectorQueryBuilder:
        """Set the number of probes to use.

        Higher values will yield better recall (more likely to find vectors if
        they exist) at the expense of latency.

        See discussion in [Querying an ANN Index][querying-an-ann-index] for
        tuning advice.

        Parameters
        ----------
        nprobes: int
            The number of probes to use.

        Returns
        -------
        LanceVectorQueryBuilder
            The LanceQueryBuilder object.
        """
        self._nprobes = nprobes
        return self

    def distance_range(
        self, lower_bound: Optional[float] = None, upper_bound: Optional[float] = None
    ) -> LanceVectorQueryBuilder:
        """Set the distance range to use.

        Only rows with distances within range [lower_bound, upper_bound)
        will be returned.

        Parameters
        ----------
        lower_bound: Optional[float]
            The lower bound of the distance range.
        upper_bound: Optional[float]
            The upper bound of the distance range.

        Returns
        -------
        LanceVectorQueryBuilder
            The LanceQueryBuilder object.
        """
        self._lower_bound = lower_bound
        self._upper_bound = upper_bound
        return self

    def ef(self, ef: int) -> LanceVectorQueryBuilder:
        """Set the number of candidates to consider during search.

        Higher values will yield better recall (more likely to find vectors if
        they exist) at the expense of latency.

        This only applies to the HNSW-related index.
        The default value is 1.5 * limit.

        Parameters
        ----------
        ef: int
            The number of candidates to consider during search.

        Returns
        -------
        LanceVectorQueryBuilder
            The LanceQueryBuilder object.
        """
        self._ef = ef
        return self

    def refine_factor(self, refine_factor: int) -> LanceVectorQueryBuilder:
        """Set the refine factor to use, increasing the number of vectors sampled.

        As an example, a refine factor of 2 will sample 2x as many vectors as
        requested, re-ranks them, and returns the top half most relevant results.

        See discussion in [Querying an ANN Index][querying-an-ann-index] for
        tuning advice.

        Parameters
        ----------
        refine_factor: int
            The refine factor to use.

        Returns
        -------
        LanceVectorQueryBuilder
            The LanceQueryBuilder object.
        """
        self._refine_factor = refine_factor
        return self

    def to_arrow(self) -> pa.Table:
        """
        Execute the query and return the results as an
        [Apache Arrow Table](https://arrow.apache.org/docs/python/generated/pyarrow.Table.html#pyarrow.Table).

        In addition to the selected columns, LanceDB also returns a vector
        and also the "_distance" column which is the distance between the query
        vector and the returned vectors.
        """
        return self.to_batches().read_all()

    def to_query_object(self) -> Query:
        """
        Build a Query object

        This can be used to serialize a query
        """
        vector = self._query if isinstance(self._query, list) else self._query.tolist()
        if isinstance(vector[0], np.ndarray):
            vector = [v.tolist() for v in vector]
        return Query(
            vector=vector,
            filter=self._where,
            postfilter=self._postfilter,
            limit=self._limit,
            distance_type=self._distance_type,
            columns=self._columns,
            nprobes=self._nprobes,
            lower_bound=self._lower_bound,
            upper_bound=self._upper_bound,
            refine_factor=self._refine_factor,
            vector_column=self._vector_column,
            with_row_id=self._with_row_id,
            offset=self._offset,
            fast_search=self._fast_search,
            ef=self._ef,
            bypass_vector_index=self._bypass_vector_index,
        )

    def to_batches(self, /, batch_size: Optional[int] = None) -> pa.RecordBatchReader:
        """
        Execute the query and return the result as a RecordBatchReader object.

        Parameters
        ----------
        batch_size: int
            The maximum number of selected records in a RecordBatch object.

        Returns
        -------
        pa.RecordBatchReader
        """
        vector = self._query if isinstance(self._query, list) else self._query.tolist()
        if isinstance(vector[0], np.ndarray):
            vector = [v.tolist() for v in vector]
        query = self.to_query_object()
        result_set = self._table._execute_query(query, batch_size)
        if self._reranker is not None:
            rs_table = result_set.read_all()
            result_set = self._reranker.rerank_vector(self._str_query, rs_table)
            check_reranker_result(result_set)
            # convert result_set back to RecordBatchReader
            result_set = pa.RecordBatchReader.from_batches(
                result_set.schema, result_set.to_batches()
            )

        return result_set

    def where(self, where: str, prefilter: bool = None) -> LanceVectorQueryBuilder:
        """Set the where clause.

        Parameters
        ----------
        where: str
            The where clause which is a valid SQL where clause. See
            `Lance filter pushdown <https://lancedb.github.io/lance/read_and_write.html#filter-push-down>`_
            for valid SQL expressions.
        prefilter: bool, default True
            If True, apply the filter before vector search, otherwise the
            filter is applied on the result of vector search.

        Returns
        -------
        LanceQueryBuilder
            The LanceQueryBuilder object.
        """
        self._where = where
        if prefilter is not None:
            self._postfilter = not prefilter
        return self

    def rerank(
        self, reranker: Reranker, query_string: Optional[str] = None
    ) -> LanceVectorQueryBuilder:
        """Rerank the results using the specified reranker.

        Parameters
        ----------
        reranker: Reranker
            The reranker to use.

        query_string: Optional[str]
            The query to use for reranking. This needs to be specified explicitly here
            as the query used for vector search may already be vectorized and the
            reranker requires a string query.
            This is only required if the query used for vector search is not a string.
            Note: This doesn't yet support the case where the query is multimodal or a
            list of vectors.

        Returns
        -------
        LanceVectorQueryBuilder
            The LanceQueryBuilder object.
        """
        self._reranker = reranker
        if self._str_query is None and query_string is None:
            raise ValueError(
                """
                The query used for vector search is not a string.
                In this case, the reranker query needs to be specified explicitly.
                """
            )
        if query_string is not None and not isinstance(query_string, str):
            raise ValueError("Reranking currently only supports string queries")
        self._str_query = query_string if query_string is not None else self._str_query
        return self

    def bypass_vector_index(self) -> LanceVectorQueryBuilder:
        """
        If this is called then any vector index is skipped

        An exhaustive (flat) search will be performed.  The query vector will
        be compared to every vector in the table.  At high scales this can be
        expensive.  However, this is often still useful.  For example, skipping
        the vector index can give you ground truth results which you can use to
        calculate your recall to select an appropriate value for nprobes.

        Returns
        -------
        LanceVectorQueryBuilder
            The LanceVectorQueryBuilder object.
        """
        self._bypass_vector_index = True
        return self


class LanceFtsQueryBuilder(LanceQueryBuilder):
    """A builder for full text search for LanceDB."""

    def __init__(
        self,
        table: "Table",
        query: str,
        ordering_field_name: Optional[str] = None,
        fts_columns: Optional[Union[str, List[str]]] = None,
    ):
        super().__init__(table)
        self._query = query
        self._phrase_query = False
        self.ordering_field_name = ordering_field_name
        self._reranker = None
        if isinstance(fts_columns, str):
            fts_columns = [fts_columns]
        self._fts_columns = fts_columns

    def phrase_query(self, phrase_query: bool = True) -> LanceFtsQueryBuilder:
        """Set whether to use phrase query.

        Parameters
        ----------
        phrase_query: bool, default True
            If True, then the query will be wrapped in quotes and
            double quotes replaced by single quotes.

        Returns
        -------
        LanceFtsQueryBuilder
            The LanceFtsQueryBuilder object.
        """
        self._phrase_query = phrase_query
        return self

    def to_query_object(self) -> Query:
        return Query(
            columns=self._columns,
            filter=self._where,
            limit=self._limit,
            postfilter=self._postfilter,
            with_row_id=self._with_row_id,
            full_text_query=FullTextSearchQuery(
                query=self._query, columns=self._fts_columns
            ),
            offset=self._offset,
        )

    def to_arrow(self) -> pa.Table:
        path, fs, exist = self._table._get_fts_index_path()
        if exist:
            return self.tantivy_to_arrow()

        query = self._query
        if self._phrase_query:
            raise NotImplementedError(
                "Phrase query is not yet supported in Lance FTS. "
                "Use tantivy-based index instead for now."
            )
        query = self.to_query_object()
        results = self._table._execute_query(query)
        results = results.read_all()
        if self._reranker is not None:
            results = self._reranker.rerank_fts(self._query, results)
            check_reranker_result(results)
        return results

    def to_batches(self, /, batch_size: Optional[int] = None):
        raise NotImplementedError("to_batches on an FTS query")

    def tantivy_to_arrow(self) -> pa.Table:
        try:
            import tantivy
        except ImportError:
            raise ImportError(
                "Please install tantivy-py `pip install tantivy` to use the full text search feature."  # noqa: E501
            )

        from .fts import search_index

        # get the index path
        path, fs, exist = self._table._get_fts_index_path()

        # check if the index exist
        if not exist:
            raise FileNotFoundError(
                "Fts index does not exist. "
                "Please first call table.create_fts_index(['<field_names>']) to "
                "create the fts index."
            )

        # Check that we are on local filesystem
        if not isinstance(fs, pa_fs.LocalFileSystem):
            raise NotImplementedError(
                "Tantivy-based full text search "
                "is only supported on the local filesystem"
            )
        # open the index
        index = tantivy.Index.open(path)
        # get the scores and doc ids
        query = self._query
        if self._phrase_query:
            query = query.replace('"', "'")
            query = f'"{query}"'
        limit = self._limit if self._limit is not None else 10
        row_ids, scores = search_index(
            index, query, limit, ordering_field=self.ordering_field_name
        )
        if len(row_ids) == 0:
            empty_schema = pa.schema([pa.field("_score", pa.float32())])
            return pa.Table.from_batches([], schema=empty_schema)
        scores = pa.array(scores)
        output_tbl = self._table.to_lance().take(row_ids, columns=self._columns)
        output_tbl = output_tbl.append_column("_score", scores)
        # this needs to match vector search results which are uint64
        row_ids = pa.array(row_ids, type=pa.uint64())

        if self._where is not None:
            tmp_name = "__lancedb__duckdb__indexer__"
            output_tbl = output_tbl.append_column(
                tmp_name, pa.array(range(len(output_tbl)))
            )
            try:
                # TODO would be great to have Substrait generate pyarrow compute
                # expressions or conversely have pyarrow support SQL expressions
                # using Substrait
                import duckdb

                indexer = duckdb.sql(
                    f"SELECT {tmp_name} FROM output_tbl WHERE {self._where}"
                ).to_arrow_table()[tmp_name]
                output_tbl = output_tbl.take(indexer).drop([tmp_name])
                row_ids = row_ids.take(indexer)

            except ImportError:
                import tempfile

                import lance

                # TODO Use "memory://" instead once that's supported
                with tempfile.TemporaryDirectory() as tmp:
                    ds = lance.write_dataset(output_tbl, tmp)
                    output_tbl = ds.to_table(filter=self._where)
                    indexer = output_tbl[tmp_name]
                    row_ids = row_ids.take(indexer)
                    output_tbl = output_tbl.drop([tmp_name])

        if self._with_row_id:
            output_tbl = output_tbl.append_column("_rowid", row_ids)

        if self._reranker is not None:
            output_tbl = self._reranker.rerank_fts(self._query, output_tbl)
        return output_tbl

    def rerank(self, reranker: Reranker) -> LanceFtsQueryBuilder:
        """Rerank the results using the specified reranker.

        Parameters
        ----------
        reranker: Reranker
            The reranker to use.

        Returns
        -------
        LanceFtsQueryBuilder
            The LanceQueryBuilder object.
        """
        self._reranker = reranker
        return self


class LanceEmptyQueryBuilder(LanceQueryBuilder):
    def to_arrow(self) -> pa.Table:
        return self.to_batches().read_all()

    def to_query_object(self) -> Query:
        return Query(
            columns=self._columns,
            filter=self._where,
            limit=self._limit,
            with_row_id=self._with_row_id,
            offset=self._offset,
        )

    def to_batches(self, /, batch_size: Optional[int] = None) -> pa.RecordBatchReader:
        query = self.to_query_object()
        return self._table._execute_query(query, batch_size)

    def rerank(self, reranker: Reranker) -> LanceEmptyQueryBuilder:
        """Rerank the results using the specified reranker.

        Parameters
        ----------
        reranker: Reranker
            The reranker to use.

        Returns
        -------
        LanceEmptyQueryBuilder
            The LanceQueryBuilder object.
        """
        raise NotImplementedError("Reranking is not yet supported.")


class LanceHybridQueryBuilder(LanceQueryBuilder):
    """
    A query builder that performs hybrid vector and full text search.
    Results are combined and reranked based on the specified reranker.
    By default, the results are reranked using the RRFReranker, which
    uses reciprocal rank fusion score for reranking.

    To make the vector and fts results comparable, the scores are normalized.
    Instead of normalizing scores, the `normalize` parameter can be set to "rank"
    in the `rerank` method to convert the scores to ranks and then normalize them.
    """

    def __init__(
        self,
        table: "Table",
        query: Optional[str] = None,
        vector_column: Optional[str] = None,
        fts_columns: Optional[Union[str, List[str]]] = None,
    ):
        super().__init__(table)
        self._query = query
        self._vector_column = vector_column
        self._fts_columns = fts_columns
        self._norm = None
        self._reranker = None
        self._nprobes = None
        self._refine_factor = None
        self._distance_type = None
        self._phrase_query = None

    def _validate_query(self, query, vector=None, text=None):
        if query is not None and (vector is not None or text is not None):
            raise ValueError(
                "You can either provide a string query in search() method"
                "or set `vector()` and `text()` explicitly for hybrid search."
                "But not both."
            )

        vector_query = vector if vector is not None else query
        if not isinstance(vector_query, (str, list, np.ndarray)):
            raise ValueError("Vector query must be either a string or a vector")

        text_query = text or query
        if text_query is None:
            raise ValueError("Text query must be provided for hybrid search.")
        if not isinstance(text_query, str):
            raise ValueError("Text query must be a string")

        return vector_query, text_query

    def phrase_query(self, phrase_query: bool = None) -> LanceHybridQueryBuilder:
        """Set whether to use phrase query.

        Parameters
        ----------
        phrase_query: bool, default True
            If True, then the query will be wrapped in quotes and
            double quotes replaced by single quotes.

        Returns
        -------
        LanceHybridQueryBuilder
            The LanceHybridQueryBuilder object.
        """
        self._phrase_query = phrase_query
        return self

    def to_query_object(self) -> Query:
        raise NotImplementedError("to_query_object not yet supported on a hybrid query")

    def to_arrow(self) -> pa.Table:
        vector_query, fts_query = self._validate_query(
            self._query, self._vector, self._text
        )
        self._fts_query = LanceFtsQueryBuilder(
            self._table, fts_query, fts_columns=self._fts_columns
        )
        vector_query = self._query_to_vector(
            self._table, vector_query, self._vector_column
        )
        self._vector_query = LanceVectorQueryBuilder(
            self._table, vector_query, self._vector_column
        )

        if self._limit:
            self._vector_query.limit(self._limit)
            self._fts_query.limit(self._limit)
        if self._columns:
            self._vector_query.select(self._columns)
            self._fts_query.select(self._columns)
        if self._where:
            self._vector_query.where(self._where, self._postfilter)
            self._fts_query.where(self._where, self._postfilter)
        if self._with_row_id:
            self._vector_query.with_row_id(True)
            self._fts_query.with_row_id(True)
        if self._phrase_query:
            self._fts_query.phrase_query(True)
        if self._distance_type:
            self._vector_query.metric(self._distance_type)
        if self._nprobes:
            self._vector_query.nprobes(self._nprobes)
        if self._refine_factor:
            self._vector_query.refine_factor(self._refine_factor)
        if self._ef:
            self._vector_query.ef(self._ef)
        if self._bypass_vector_index:
            self._vector_query.bypass_vector_index()

        if self._reranker is None:
            self._reranker = RRFReranker()

        with ThreadPoolExecutor() as executor:
            fts_future = executor.submit(self._fts_query.with_row_id(True).to_arrow)
            vector_future = executor.submit(
                self._vector_query.with_row_id(True).to_arrow
            )
            fts_results = fts_future.result()
            vector_results = vector_future.result()

        return self._combine_hybrid_results(
            fts_results=fts_results,
            vector_results=vector_results,
            norm=self._norm,
            fts_query=self._fts_query._query,
            reranker=self._reranker,
            limit=self._limit,
            with_row_ids=self._with_row_id,
        )

    @staticmethod
    def _combine_hybrid_results(
        fts_results: pa.Table,
        vector_results: pa.Table,
        norm: str,
        fts_query: str,
        reranker,
        limit: int,
        with_row_ids: bool,
    ) -> pa.Table:
        if norm == "rank":
            vector_results = LanceHybridQueryBuilder._rank(vector_results, "_distance")
            fts_results = LanceHybridQueryBuilder._rank(fts_results, "_score")

        original_distances = None
        original_scores = None
        original_distance_row_ids = None
        original_score_row_ids = None
        # normalize the scores to be between 0 and 1, 0 being most relevant
        # We check whether the results (vector and FTS) are empty, because when
        # they are, they often are missing the _rowid column, which causes an error
        if vector_results.num_rows > 0:
            distance_i = vector_results.column_names.index("_distance")
            original_distances = vector_results.column(distance_i)
            original_distance_row_ids = vector_results.column("_rowid")
            vector_results = vector_results.set_column(
                distance_i,
                vector_results.field(distance_i),
                LanceHybridQueryBuilder._normalize_scores(original_distances),
            )

        # In fts higher scores represent relevance. Not inverting them here as
        # rerankers might need to preserve this score to support `return_score="all"`
        if fts_results.num_rows > 0:
            score_i = fts_results.column_names.index("_score")
            original_scores = fts_results.column(score_i)
            original_score_row_ids = fts_results.column("_rowid")
            fts_results = fts_results.set_column(
                score_i,
                fts_results.field(score_i),
                LanceHybridQueryBuilder._normalize_scores(original_scores),
            )

        results = reranker.rerank_hybrid(fts_query, vector_results, fts_results)

        check_reranker_result(results)

        if "_distance" in results.column_names and original_distances is not None:
            # restore the original distances
            indices = pc.index_in(
                results["_rowid"], original_distance_row_ids, skip_nulls=True
            )
            original_distances = pc.take(original_distances, indices)
            distance_i = results.column_names.index("_distance")
            results = results.set_column(distance_i, "_distance", original_distances)

        if "_score" in results.column_names and original_scores is not None:
            # restore the original scores
            indices = pc.index_in(
                results["_rowid"], original_score_row_ids, skip_nulls=True
            )
            original_scores = pc.take(original_scores, indices)
            score_i = results.column_names.index("_score")
            results = results.set_column(score_i, "_score", original_scores)

        results = results.slice(length=limit)

        if not with_row_ids:
            results = results.drop(["_rowid"])

        return results

    def to_batches(self):
        raise NotImplementedError("to_batches not yet supported on a hybrid query")

    @staticmethod
    def _rank(results: pa.Table, column: str, ascending: bool = True):
        if len(results) == 0:
            return results
        # Get the _score column from results
        scores = results.column(column).to_numpy()
        sort_indices = np.argsort(scores)
        if not ascending:
            sort_indices = sort_indices[::-1]
        ranks = np.empty_like(sort_indices)
        ranks[sort_indices] = np.arange(len(scores)) + 1
        # replace the _score column with the ranks
        _score_idx = results.column_names.index(column)
        results = results.set_column(
            _score_idx, column, pa.array(ranks, type=pa.float32())
        )
        return results

    @staticmethod
    def _normalize_scores(scores: pa.Array, invert=False) -> pa.Array:
        if len(scores) == 0:
            return scores
        # normalize the scores by subtracting the min and dividing by the max
        min, max = pc.min_max(scores).values()
        rng = pc.subtract(max, min)

        if not pc.equal(rng, pa.scalar(0.0)).as_py():
            scores = pc.divide(pc.subtract(scores, min), rng)
        elif not pc.equal(max, pa.scalar(0.0)).as_py():
            # If rng is 0, then we at least want the scores to be 0
            scores = pc.subtract(scores, min)

        if invert:
            scores = pc.subtract(1, scores)

        return scores

    def rerank(
        self,
        reranker: Reranker = RRFReranker(),
        normalize: str = "score",
    ) -> LanceHybridQueryBuilder:
        """
        Rerank the hybrid search results using the specified reranker. The reranker
        must be an instance of Reranker class.

        Parameters
        ----------
        reranker: Reranker, default RRFReranker()
            The reranker to use. Must be an instance of Reranker class.
        normalize: str, default "score"
            The method to normalize the scores. Can be "rank" or "score". If "rank",
            the scores are converted to ranks and then normalized. If "score", the
            scores are normalized directly.
        Returns
        -------
        LanceHybridQueryBuilder
            The LanceHybridQueryBuilder object.
        """
        if normalize not in ["rank", "score"]:
            raise ValueError("normalize must be 'rank' or 'score'.")
        if reranker and not isinstance(reranker, Reranker):
            raise ValueError("reranker must be an instance of Reranker class.")

        self._norm = normalize
        self._reranker = reranker

        return self

    def nprobes(self, nprobes: int) -> LanceHybridQueryBuilder:
        """
        Set the number of probes to use for vector search.

        Higher values will yield better recall (more likely to find vectors if
        they exist) at the expense of latency.

        Parameters
        ----------
        nprobes: int
            The number of probes to use.

        Returns
        -------
        LanceHybridQueryBuilder
            The LanceHybridQueryBuilder object.
        """
        self._nprobes = nprobes
        return self

    def distance_range(
        self, lower_bound: Optional[float] = None, upper_bound: Optional[float] = None
    ) -> LanceHybridQueryBuilder:
        """
        Set the distance range to use.

        Only rows with distances within range [lower_bound, upper_bound)
        will be returned.

        Parameters
        ----------
        lower_bound: Optional[float]
            The lower bound of the distance range.
        upper_bound: Optional[float]
            The upper bound of the distance range.

        Returns
        -------
        LanceHybridQueryBuilder
            The LanceHybridQueryBuilder object.
        """
        self._lower_bound = lower_bound
        self._upper_bound = upper_bound
        return self

    def ef(self, ef: int) -> LanceHybridQueryBuilder:
        """
        Set the number of candidates to consider during search.

        Higher values will yield better recall (more likely to find vectors if
        they exist) at the expense of latency.

        This only applies to the HNSW-related index.
        The default value is 1.5 * limit.

        Parameters
        ----------
        ef: int
            The number of candidates to consider during search.

        Returns
        -------
        LanceHybridQueryBuilder
            The LanceHybridQueryBuilder object.
        """
        self._ef = ef
        return self

    def metric(self, metric: Literal["l2", "cosine", "dot"]) -> LanceHybridQueryBuilder:
        """Set the distance metric to use.

        This is an alias for distance_type() and may be deprecated in the future.
        Please use distance_type() instead.

        Parameters
        ----------
        metric: "l2" or "cosine" or "dot"
            The distance metric to use. By default "l2" is used.

        Returns
        -------
        LanceVectorQueryBuilder
            The LanceQueryBuilder object.
        """
        return self.distance_type(metric)

    def distance_type(
        self, distance_type: Literal["l2", "cosine", "dot"]
    ) -> "LanceHybridQueryBuilder":
        """Set the distance metric to use.

        When performing a vector search we try and find the "nearest" vectors according
        to some kind of distance metric. This parameter controls which distance metric
        to use.

        Note: if there is a vector index then the distance type used MUST match the
        distance type used to train the vector index. If this is not done then the
        results will be invalid.

        Parameters
        ----------
        distance_type: "l2" or "cosine" or "dot"
            The distance metric to use. By default "l2" is used.

        Returns
        -------
        LanceVectorQueryBuilder
            The LanceQueryBuilder object.
        """
        self._distance_type = distance_type.lower()
        return self

    def refine_factor(self, refine_factor: int) -> LanceHybridQueryBuilder:
        """
        Refine the vector search results by reading extra elements and
        re-ranking them in memory.

        Parameters
        ----------
        refine_factor: int
            The refine factor to use.

        Returns
        -------
        LanceHybridQueryBuilder
            The LanceHybridQueryBuilder object.
        """
        self._refine_factor = refine_factor
        return self

    def vector(self, vector: Union[np.ndarray, list]) -> LanceHybridQueryBuilder:
        self._vector = vector
        return self

    def text(self, text: str) -> LanceHybridQueryBuilder:
        self._text = text
        return self

    def bypass_vector_index(self) -> LanceHybridQueryBuilder:
        """
        If this is called then any vector index is skipped

        An exhaustive (flat) search will be performed.  The query vector will
        be compared to every vector in the table.  At high scales this can be
        expensive.  However, this is often still useful.  For example, skipping
        the vector index can give you ground truth results which you can use to
        calculate your recall to select an appropriate value for nprobes.

        Returns
        -------
        LanceHybridQueryBuilder
            The LanceHybridQueryBuilder object.
        """
        self._bypass_vector_index = True
        return self


class AsyncQueryBase(object):
    def __init__(self, inner: Union[LanceQuery, LanceVectorQuery]):
        """
        Construct an AsyncQueryBase

        This method is not intended to be called directly.  Instead, use the
        [AsyncTable.query][lancedb.table.AsyncTable.query] method to create a query.
        """
        self._inner = inner

    def to_query_object(self) -> Query:
        return Query.from_inner(self._inner.to_query_request())

    def where(self, predicate: str) -> Self:
        """
        Only return rows matching the given predicate

        The predicate should be supplied as an SQL query string.

        Examples
        --------

        >>> predicate = "x > 10"
        >>> predicate = "y > 0 AND y < 100"
        >>> predicate = "x > 5 OR y = 'test'"

        Filtering performance can often be improved by creating a scalar index
        on the filter column(s).
        """
        self._inner.where(predicate)
        return self

    def select(self, columns: Union[List[str], dict[str, str]]) -> Self:
        """
        Return only the specified columns.

        By default a query will return all columns from the table.  However, this can
        have a very significant impact on latency.  LanceDb stores data in a columnar
        fashion.  This
        means we can finely tune our I/O to select exactly the columns we need.

        As a best practice you should always limit queries to the columns that you need.
        If you pass in a list of column names then only those columns will be
        returned.

        You can also use this method to create new "dynamic" columns based on your
        existing columns. For example, you may not care about "a" or "b" but instead
        simply want "a + b".  This is often seen in the SELECT clause of an SQL query
        (e.g. `SELECT a+b FROM my_table`).

        To create dynamic columns you can pass in a dict[str, str].  A column will be
        returned for each entry in the map.  The key provides the name of the column.
        The value is an SQL string used to specify how the column is calculated.

        For example, an SQL query might state `SELECT a + b AS combined, c`.  The
        equivalent input to this method would be `{"combined": "a + b", "c": "c"}`.

        Columns will always be returned in the order given, even if that order is
        different than the order used when adding the data.
        """
        if isinstance(columns, list) and all(isinstance(c, str) for c in columns):
            self._inner.select_columns(columns)
        elif isinstance(columns, dict) and all(
            isinstance(k, str) and isinstance(v, str) for k, v in columns.items()
        ):
            self._inner.select(list(columns.items()))
        else:
            raise TypeError("columns must be a list of column names or a dict")
        return self

    def limit(self, limit: int) -> Self:
        """
        Set the maximum number of results to return.

        By default, a plain search has no limit.  If this method is not
        called then every valid row from the table will be returned.
        """
        self._inner.limit(limit)
        return self

    def offset(self, offset: int) -> Self:
        """
        Set the offset for the results.

        Parameters
        ----------
        offset: int
            The offset to start fetching results from.
        """
        self._inner.offset(offset)
        return self

    def fast_search(self) -> Self:
        """
        Skip searching un-indexed data.

        This can make queries faster, but will miss any data that has not been
        indexed.

        !!! tip
            You can add new data into an existing index by calling
            [AsyncTable.optimize][lancedb.table.AsyncTable.optimize].
        """
        self._inner.fast_search()
        return self

    def with_row_id(self) -> Self:
        """
        Include the _rowid column in the results.
        """
        self._inner.with_row_id()
        return self

    def postfilter(self) -> Self:
        """
        If this is called then filtering will happen after the search instead of
        before.
        By default filtering will be performed before the search.  This is how
        filtering is typically understood to work.  This prefilter step does add some
        additional latency.  Creating a scalar index on the filter column(s) can
        often improve this latency.  However, sometimes a filter is too complex or
        scalar indices cannot be applied to the column.  In these cases postfiltering
        can be used instead of prefiltering to improve latency.
        Post filtering applies the filter to the results of the search.  This
        means we only run the filter on a much smaller set of data.  However, it can
        cause the query to return fewer than `limit` results (or even no results) if
        none of the nearest results match the filter.
        Post filtering happens during the "refine stage" (described in more detail in
        @see {@link VectorQuery#refineFactor}).  This means that setting a higher refine
        factor can often help restore some of the results lost by post filtering.
        """
        self._inner.postfilter()
        return self

    async def to_batches(
        self, *, max_batch_length: Optional[int] = None
    ) -> AsyncRecordBatchReader:
        """
        Execute the query and return the results as an Apache Arrow RecordBatchReader.

        Parameters
        ----------

        max_batch_length: Optional[int]
            The maximum number of selected records in a single RecordBatch object.
            If not specified, a default batch length is used.
            It is possible for batches to be smaller than the provided length if the
            underlying data is stored in smaller chunks.
        """
        return AsyncRecordBatchReader(await self._inner.execute(max_batch_length))

    async def to_arrow(self) -> pa.Table:
        """
        Execute the query and collect the results into an Apache Arrow Table.

        This method will collect all results into memory before returning.  If
        you expect a large number of results, you may want to use
        [to_batches][lancedb.query.AsyncQueryBase.to_batches]
        """
        batch_iter = await self.to_batches()
        return pa.Table.from_batches(
            await batch_iter.read_all(), schema=batch_iter.schema
        )

    async def to_list(self) -> List[dict]:
        """
        Execute the query and return the results as a list of dictionaries.

        Each list entry is a dictionary with the selected column names as keys,
        or all table columns if `select` is not called. The vector and the "_distance"
        fields are returned whether or not they're explicitly selected.
        """
        return (await self.to_arrow()).to_pylist()

    async def to_pandas(
        self, flatten: Optional[Union[int, bool]] = None
    ) -> "pd.DataFrame":
        """
        Execute the query and collect the results into a pandas DataFrame.

        This method will collect all results into memory before returning.  If you
        expect a large number of results, you may want to use
        [to_batches][lancedb.query.AsyncQueryBase.to_batches] and convert each batch to
        pandas separately.

        Examples
        --------

        >>> import asyncio
        >>> from lancedb import connect_async
        >>> async def doctest_example():
        ...     conn = await connect_async("./.lancedb")
        ...     table = await conn.create_table("my_table", data=[{"a": 1, "b": 2}])
        ...     async for batch in await table.query().to_batches():
        ...         batch_df = batch.to_pandas()
        >>> asyncio.run(doctest_example())

        Parameters
        ----------
        flatten: Optional[Union[int, bool]]
            If flatten is True, flatten all nested columns.
            If flatten is an integer, flatten the nested columns up to the
            specified depth.
            If unspecified, do not flatten the nested columns.
        """
        return (flatten_columns(await self.to_arrow(), flatten)).to_pandas()

    async def to_polars(self) -> "pl.DataFrame":
        """
        Execute the query and collect the results into a Polars DataFrame.

        This method will collect all results into memory before returning.  If you
        expect a large number of results, you may want to use
        [to_batches][lancedb.query.AsyncQueryBase.to_batches] and convert each batch to
        polars separately.

        Examples
        --------

        >>> import asyncio
        >>> import polars as pl
        >>> from lancedb import connect_async
        >>> async def doctest_example():
        ...     conn = await connect_async("./.lancedb")
        ...     table = await conn.create_table("my_table", data=[{"a": 1, "b": 2}])
        ...     async for batch in await table.query().to_batches():
        ...         batch_df = pl.from_arrow(batch)
        >>> asyncio.run(doctest_example())
        """
        import polars as pl

        return pl.from_arrow(await self.to_arrow())

    async def explain_plan(self, verbose: Optional[bool] = False):
        """Return the execution plan for this query.

        Examples
        --------
        >>> import asyncio
        >>> from lancedb import connect_async
        >>> async def doctest_example():
        ...     conn = await connect_async("./.lancedb")
        ...     table = await conn.create_table("my_table", [{"vector": [99, 99]}])
        ...     query = [100, 100]
        ...     plan = await table.query().nearest_to([1, 2]).explain_plan(True)
        ...     print(plan)
        >>> asyncio.run(doctest_example()) # doctest: +ELLIPSIS, +NORMALIZE_WHITESPACE
        ProjectionExec: expr=[vector@0 as vector, _distance@2 as _distance]
          GlobalLimitExec: skip=0, fetch=10
            FilterExec: _distance@2 IS NOT NULL
              SortExec: TopK(fetch=10), expr=[_distance@2 ASC NULLS LAST], preserve_partitioning=[false]
                KNNVectorDistance: metric=l2
                  LanceScan: uri=..., projection=[vector], row_id=true, row_addr=false, ordered=false

        Parameters
        ----------
        verbose : bool, default False
            Use a verbose output format.

        Returns
        -------
        plan : str
        """  # noqa: E501
        return await self._inner.explain_plan(verbose)


class AsyncQuery(AsyncQueryBase):
    def __init__(self, inner: LanceQuery):
        """
        Construct an AsyncQuery

        This method is not intended to be called directly.  Instead, use the
        [AsyncTable.query][lancedb.table.AsyncTable.query] method to create a query.
        """
        super().__init__(inner)
        self._inner = inner

    @classmethod
    def _query_vec_to_array(self, vec: Union[VEC, Tuple]):
        if isinstance(vec, list):
            return pa.array(vec)
        if isinstance(vec, np.ndarray):
            return pa.array(vec)
        if isinstance(vec, pa.Array):
            return vec
        if isinstance(vec, pa.ChunkedArray):
            return vec.combine_chunks()
        if isinstance(vec, tuple):
            return pa.array(vec)
        # We've checked everything we formally support in our typings
        # but, as a fallback, let pyarrow try and convert it anyway.
        # This can allow for some more exotic things like iterables
        return pa.array(vec)

    def nearest_to(
        self,
        query_vector: Union[VEC, Tuple, List[VEC]],
    ) -> AsyncVectorQuery:
        """
        Find the nearest vectors to the given query vector.

        This converts the query from a plain query to a vector query.

        This method will attempt to convert the input to the query vector
        expected by the embedding model.  If the input cannot be converted
        then an error will be thrown.

        By default, there is no embedding model, and the input should be
        something that can be converted to a pyarrow array of floats.  This
        includes lists, numpy arrays, and tuples.

        If there is only one vector column (a column whose data type is a
        fixed size list of floats) then the column does not need to be specified.
        If there is more than one vector column you must use
        [AsyncVectorQuery.column][lancedb.query.AsyncVectorQuery.column] to specify
        which column you would like to compare with.

        If no index has been created on the vector column then a vector query
        will perform a distance comparison between the query vector and every
        vector in the database and then sort the results.  This is sometimes
        called a "flat search"

        For small databases, with tens of thousands of vectors or less, this can
        be reasonably fast.  In larger databases you should create a vector index
        on the column.  If there is a vector index then an "approximate" nearest
        neighbor search (frequently called an ANN search) will be performed.  This
        search is much faster, but the results will be approximate.

        The query can be further parameterized using the returned builder.  There
        are various ANN search parameters that will let you fine tune your recall
        accuracy vs search latency.

        Vector searches always have a [limit][].  If `limit` has not been called then
        a default `limit` of 10 will be used.

        Typically, a single vector is passed in as the query. However, you can also
        pass in multiple vectors. When multiple vectors are passed in, if the vector
        column is with multivector type, then the vectors will be treated as a single
        query. Or the vectors will be treated as multiple queries, this can be useful
        if you want to find the nearest vectors to multiple query vectors.
        This is not expected to be faster than making multiple queries concurrently;
        it is just a convenience method. If multiple vectors are passed in then
        an additional column `query_index` will be added to the results. This column
        will contain the index of the query vector that the result is nearest to.
        """
        if query_vector is None:
            raise ValueError("query_vector can not be None")

        if (
            isinstance(query_vector, (list, np.ndarray, pa.Array))
            and len(query_vector) > 0
            and isinstance(query_vector[0], (list, np.ndarray, pa.Array))
        ):
            # multiple have been passed
            query_vectors = [AsyncQuery._query_vec_to_array(v) for v in query_vector]
            new_self = self._inner.nearest_to(query_vectors[0])
            for v in query_vectors[1:]:
                new_self.add_query_vector(v)
            return AsyncVectorQuery(new_self)
        else:
            return AsyncVectorQuery(
                self._inner.nearest_to(AsyncQuery._query_vec_to_array(query_vector))
            )

    def nearest_to_text(
        self, query: str, columns: Union[str, List[str], None] = None
    ) -> AsyncFTSQuery:
        """
        Find the documents that are most relevant to the given text query.

        This method will perform a full text search on the table and return
        the most relevant documents.  The relevance is determined by BM25.

        The columns to search must be with native FTS index
        (Tantivy-based can't work with this method).

        By default, all indexed columns are searched,
        now only one column can be searched at a time.

        Parameters
        ----------
        query: str
            The text query to search for.
        columns: str or list of str, default None
            The columns to search in. If None, all indexed columns are searched.
            For now only one column can be searched at a time.
        """
        if isinstance(columns, str):
            columns = [columns]
        if columns is None:
            columns = []
        return AsyncFTSQuery(
            self._inner.nearest_to_text({"query": query, "columns": columns})
        )


class AsyncFTSQuery(AsyncQueryBase):
    """A query for full text search for LanceDB."""

    def __init__(self, inner: LanceFTSQuery):
        super().__init__(inner)
        self._inner = inner
        self._reranker = None

    def get_query(self) -> str:
        return self._inner.get_query()

    def rerank(
        self,
        reranker: Reranker = RRFReranker(),
    ) -> AsyncFTSQuery:
        if reranker and not isinstance(reranker, Reranker):
            raise ValueError("reranker must be an instance of Reranker class.")

        self._reranker = reranker

        return self

    def nearest_to(
        self,
        query_vector: Union[VEC, Tuple, List[VEC]],
    ) -> AsyncHybridQuery:
        """
        In addition doing text search on the LanceDB Table, also
        find the nearest vectors to the given query vector.

        This converts the query from a FTS Query to a Hybrid query. Results
        from the vector search will be combined with results from the FTS query.

        This method will attempt to convert the input to the query vector
        expected by the embedding model.  If the input cannot be converted
        then an error will be thrown.

        By default, there is no embedding model, and the input should be
        something that can be converted to a pyarrow array of floats.  This
        includes lists, numpy arrays, and tuples.

        If there is only one vector column (a column whose data type is a
        fixed size list of floats) then the column does not need to be specified.
        If there is more than one vector column you must use
        [AsyncVectorQuery.column][lancedb.query.AsyncVectorQuery.column] to specify
        which column you would like to compare with.

        If no index has been created on the vector column then a vector query
        will perform a distance comparison between the query vector and every
        vector in the database and then sort the results.  This is sometimes
        called a "flat search"

        For small databases, with tens of thousands of vectors or less, this can
        be reasonably fast.  In larger databases you should create a vector index
        on the column.  If there is a vector index then an "approximate" nearest
        neighbor search (frequently called an ANN search) will be performed.  This
        search is much faster, but the results will be approximate.

        The query can be further parameterized using the returned builder.  There
        are various ANN search parameters that will let you fine tune your recall
        accuracy vs search latency.

        Hybrid searches always have a [limit][].  If `limit` has not been called then
        a default `limit` of 10 will be used.

        Typically, a single vector is passed in as the query. However, you can also
        pass in multiple vectors.  This can be useful if you want to find the nearest
        vectors to multiple query vectors. This is not expected to be faster than
        making multiple queries concurrently; it is just a convenience method.
        If multiple vectors are passed in then an additional column `query_index`
        will be added to the results.  This column will contain the index of the
        query vector that the result is nearest to.
        """
        if query_vector is None:
            raise ValueError("query_vector can not be None")

        if (
            isinstance(query_vector, list)
            and len(query_vector) > 0
            and not isinstance(query_vector[0], (float, int))
        ):
            # multiple have been passed
            query_vectors = [AsyncQuery._query_vec_to_array(v) for v in query_vector]
            new_self = self._inner.nearest_to(query_vectors[0])
            for v in query_vectors[1:]:
                new_self.add_query_vector(v)
            return AsyncHybridQuery(new_self)
        else:
            return AsyncHybridQuery(
                self._inner.nearest_to(AsyncQuery._query_vec_to_array(query_vector))
            )

    async def to_batches(
        self, *, max_batch_length: Optional[int] = None
    ) -> AsyncRecordBatchReader:
        reader = await super().to_batches()
        results = pa.Table.from_batches(await reader.read_all(), reader.schema)
        if self._reranker:
            results = self._reranker.rerank_fts(self.get_query(), results)
        return AsyncRecordBatchReader(results, max_batch_length=max_batch_length)


class AsyncVectorQueryBase:
    def column(self, column: str) -> Self:
        """
        Set the vector column to query

        This controls which column is compared to the query vector supplied in
        the call to [AsyncQuery.nearest_to][lancedb.query.AsyncQuery.nearest_to].

        This parameter must be specified if the table has more than one column
        whose data type is a fixed-size-list of floats.
        """
        self._inner.column(column)
        return self

    def nprobes(self, nprobes: int) -> Self:
        """
        Set the number of partitions to search (probe)

        This argument is only used when the vector column has an IVF-based index.
        If there is no index then this value is ignored.

        The IVF stage of IVF PQ divides the input into partitions (clusters) of
        related values.

        The partition whose centroids are closest to the query vector will be
        exhaustiely searched to find matches.  This parameter controls how many
        partitions should be searched.

        Increasing this value will increase the recall of your query but will
        also increase the latency of your query.  The default value is 20.  This
        default is good for many cases but the best value to use will depend on
        your data and the recall that you need to achieve.

        For best results we recommend tuning this parameter with a benchmark against
        your actual data to find the smallest possible value that will still give
        you the desired recall.
        """
        self._inner.nprobes(nprobes)
        return self

    def distance_range(
        self, lower_bound: Optional[float] = None, upper_bound: Optional[float] = None
    ) -> Self:
        """Set the distance range to use.

        Only rows with distances within range [lower_bound, upper_bound)
        will be returned.

        Parameters
        ----------
        lower_bound: Optional[float]
            The lower bound of the distance range.
        upper_bound: Optional[float]
            The upper bound of the distance range.

        Returns
        -------
        AsyncVectorQuery
            The AsyncVectorQuery object.
        """
        self._inner.distance_range(lower_bound, upper_bound)
        return self

    def ef(self, ef: int) -> Self:
        """
        Set the number of candidates to consider during search

        This argument is only used when the vector column has an HNSW index.
        If there is no index then this value is ignored.

        Increasing this value will increase the recall of your query but will also
        increase the latency of your query.  The default value is 1.5 * limit.  This
        default is good for many cases but the best value to use will depend on your
        data and the recall that you need to achieve.
        """
        self._inner.ef(ef)
        return self

    def refine_factor(self, refine_factor: int) -> Self:
        """
        A multiplier to control how many additional rows are taken during the refine
        step

        This argument is only used when the vector column has an IVF PQ index.
        If there is no index then this value is ignored.

        An IVF PQ index stores compressed (quantized) values.  They query vector is
        compared against these values and, since they are compressed, the comparison is
        inaccurate.

        This parameter can be used to refine the results.  It can improve both improve
        recall and correct the ordering of the nearest results.

        To refine results LanceDb will first perform an ANN search to find the nearest
        `limit` * `refine_factor` results.  In other words, if `refine_factor` is 3 and
        `limit` is the default (10) then the first 30 results will be selected.  LanceDb
        then fetches the full, uncompressed, values for these 30 results.  The results
        are then reordered by the true distance and only the nearest 10 are kept.

        Note: there is a difference between calling this method with a value of 1 and
        never calling this method at all.  Calling this method with any value will have
        an impact on your search latency.  When you call this method with a
        `refine_factor` of 1 then LanceDb still needs to fetch the full, uncompressed,
        values so that it can potentially reorder the results.

        Note: if this method is NOT called then the distances returned in the _distance
        column will be approximate distances based on the comparison of the quantized
        query vector and the quantized result vectors.  This can be considerably
        different than the true distance between the query vector and the actual
        uncompressed vector.
        """
        self._inner.refine_factor(refine_factor)
        return self

    def distance_type(self, distance_type: str) -> Self:
        """
        Set the distance metric to use

        When performing a vector search we try and find the "nearest" vectors according
        to some kind of distance metric.  This parameter controls which distance metric
        to use.  See @see {@link IvfPqOptions.distanceType} for more details on the
        different distance metrics available.

        Note: if there is a vector index then the distance type used MUST match the
        distance type used to train the vector index.  If this is not done then the
        results will be invalid.

        By default "l2" is used.
        """
        self._inner.distance_type(distance_type)
        return self

    def bypass_vector_index(self) -> Self:
        """
        If this is called then any vector index is skipped

        An exhaustive (flat) search will be performed.  The query vector will
        be compared to every vector in the table.  At high scales this can be
        expensive.  However, this is often still useful.  For example, skipping
        the vector index can give you ground truth results which you can use to
        calculate your recall to select an appropriate value for nprobes.
        """
        self._inner.bypass_vector_index()
        return self


class AsyncVectorQuery(AsyncQueryBase, AsyncVectorQueryBase):
    def __init__(self, inner: LanceVectorQuery):
        """
        Construct an AsyncVectorQuery

        This method is not intended to be called directly.  Instead, create
        a query first with [AsyncTable.query][lancedb.table.AsyncTable.query] and then
        use [AsyncQuery.nearest_to][lancedb.query.AsyncQuery.nearest_to]] to convert to
        a vector query.  Or you can use
        [AsyncTable.vector_search][lancedb.table.AsyncTable.vector_search]
        """
        super().__init__(inner)
        self._inner = inner
        self._reranker = None
        self._query_string = None

    def rerank(
        self, reranker: Reranker = RRFReranker(), query_string: Optional[str] = None
    ) -> AsyncHybridQuery:
        if reranker and not isinstance(reranker, Reranker):
            raise ValueError("reranker must be an instance of Reranker class.")

        self._reranker = reranker

        if not self._query_string and not query_string:
            raise ValueError("query_string must be provided to rerank the results.")

        self._query_string = query_string

        return self

    def nearest_to_text(
        self, query: str, columns: Union[str, List[str], None] = None
    ) -> AsyncHybridQuery:
        """
        Find the documents that are most relevant to the given text query,
        in addition to vector search.

        This converts the vector query into a hybrid query.

        This search will perform a full text search on the table and return
        the most relevant documents, combined with the vector query results.
        The text relevance is determined by BM25.

        The columns to search must be with native FTS index
        (Tantivy-based can't work with this method).

        By default, all indexed columns are searched,
        now only one column can be searched at a time.

        Parameters
        ----------
        query: str
            The text query to search for.
        columns: str or list of str, default None
            The columns to search in. If None, all indexed columns are searched.
            For now only one column can be searched at a time.
        """
        if isinstance(columns, str):
            columns = [columns]
        if columns is None:
            columns = []
        return AsyncHybridQuery(
            self._inner.nearest_to_text({"query": query, "columns": columns})
        )

    async def to_batches(
        self, *, max_batch_length: Optional[int] = None
    ) -> AsyncRecordBatchReader:
        reader = await super().to_batches()
        results = pa.Table.from_batches(await reader.read_all(), reader.schema)
        if self._reranker:
            results = self._reranker.rerank_vector(self._query_string, results)
        return AsyncRecordBatchReader(results, max_batch_length=max_batch_length)


class AsyncHybridQuery(AsyncQueryBase, AsyncVectorQueryBase):
    """
    A query builder that performs hybrid vector and full text search.
    Results are combined and reranked based on the specified reranker.
    By default, the results are reranked using the RRFReranker, which
    uses reciprocal rank fusion score for reranking.

    To make the vector and fts results comparable, the scores are normalized.
    Instead of normalizing scores, the `normalize` parameter can be set to "rank"
    in the `rerank` method to convert the scores to ranks and then normalize them.
    """

    def __init__(self, inner: LanceHybridQuery):
        super().__init__(inner)
        self._inner = inner
        self._norm = "score"
        self._reranker = RRFReranker()

    def rerank(
        self, reranker: Reranker = RRFReranker(), normalize: str = "score"
    ) -> AsyncHybridQuery:
        """
        Rerank the hybrid search results using the specified reranker. The reranker
        must be an instance of Reranker class.

        Parameters
        ----------
        reranker: Reranker, default RRFReranker()
            The reranker to use. Must be an instance of Reranker class.
        normalize: str, default "score"
            The method to normalize the scores. Can be "rank" or "score". If "rank",
            the scores are converted to ranks and then normalized. If "score", the
            scores are normalized directly.
        Returns
        -------
        AsyncHybridQuery
            The AsyncHybridQuery object.
        """
        if normalize not in ["rank", "score"]:
            raise ValueError("normalize must be 'rank' or 'score'.")
        if reranker and not isinstance(reranker, Reranker):
            raise ValueError("reranker must be an instance of Reranker class.")

        self._norm = normalize
        self._reranker = reranker

        return self

    async def to_batches(
        self, *, max_batch_length: Optional[int] = None
    ) -> AsyncRecordBatchReader:
        fts_query = AsyncFTSQuery(self._inner.to_fts_query())
        vec_query = AsyncVectorQuery(self._inner.to_vector_query())

        # save the row ID choice that was made on the query builder and force it
        # to actually fetch the row ids because we need this for reranking
        with_row_ids = self._inner.get_with_row_id()
        fts_query.with_row_id()
        vec_query.with_row_id()

        fts_results, vector_results = await asyncio.gather(
            fts_query.to_arrow(),
            vec_query.to_arrow(),
        )

        result = LanceHybridQueryBuilder._combine_hybrid_results(
            fts_results=fts_results,
            vector_results=vector_results,
            norm=self._norm,
            fts_query=fts_query.get_query(),
            reranker=self._reranker,
            limit=self._inner.get_limit(),
            with_row_ids=with_row_ids,
        )

        return AsyncRecordBatchReader(result, max_batch_length=max_batch_length)

    async def explain_plan(self, verbose: Optional[bool] = False):
        """Return the execution plan for this query.

        The output includes both the vector and FTS search plans.

        Examples
        --------
        >>> import asyncio
        >>> from lancedb import connect_async
        >>> from lancedb.index import FTS
        >>> async def doctest_example():
        ...     conn = await connect_async("./.lancedb")
        ...     table = await conn.create_table("my_table", [{"vector": [99, 99], "text": "hello world"}])
        ...     await table.create_index("text", config=FTS(with_position=False))
        ...     query = [100, 100]
        ...     plan = await table.query().nearest_to([1, 2]).nearest_to_text("hello").explain_plan(True)
        ...     print(plan)
        >>> asyncio.run(doctest_example()) # doctest: +ELLIPSIS, +NORMALIZE_WHITESPACE
        Vector Search Plan:
        ProjectionExec: expr=[vector@0 as vector, text@3 as text, _distance@2 as _distance]
            Take: columns="vector, _rowid, _distance, (text)"
                CoalesceBatchesExec: target_batch_size=1024
                GlobalLimitExec: skip=0, fetch=10
                    FilterExec: _distance@2 IS NOT NULL
                    SortExec: TopK(fetch=10), expr=[_distance@2 ASC NULLS LAST], preserve_partitioning=[false]
                        KNNVectorDistance: metric=l2
                        LanceScan: uri=..., projection=[vector], row_id=true, row_addr=false, ordered=false
        FTS Search Plan:
        LanceScan: uri=..., projection=[vector, text], row_id=false, row_addr=false, ordered=true

        Parameters
        ----------
        verbose : bool, default False
            Use a verbose output format.

        Returns
        -------
        plan
        """  # noqa: E501

        results = ["Vector Search Plan:"]
        results.append(await self._inner.to_vector_query().explain_plan(verbose))
        results.append("FTS Search Plan:")
        results.append(await self._inner.to_fts_query().explain_plan(verbose))

        return "\n".join(results)
