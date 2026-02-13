# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors


from typing import Union, List, TYPE_CHECKING
import pyarrow as pa

from collections import defaultdict
from .base import Reranker

if TYPE_CHECKING:
    from ..table import LanceVectorQueryBuilder


class RRFReranker(Reranker):
    """
    Rerank search results using Reciprocal Rank Fusion (RRF).

    RRF is a simple yet effective algorithm for combining ranked lists.
    It can be used for:

    - Vector search reranking
    - Full-text search reranking
    - Hybrid search (vector + FTS) reranking

    The algorithm assigns scores based on rank position:

        score = 1 / (rank + K)

    where rank is the 0-indexed position and K is a constant (default 60).

    Parameters
    ----------
    K : int, default 60
        The constant used in RRF formula. Research suggests K=60 is
        near-optimal, but the choice is not critical.
    return_score : str, default "relevance"
        Whether to return only "_relevance_score" or "all" scores
        including "_distance" and "_score".

    Examples
    --------
    >>> reranker = RRFReranker(K=60)
    >>> # Vector search with reranking
    >>> results = table.search(query_vector).rerank(reranker).to_arrow()
    >>> # Hybrid search with reranking
    >>> results = table.search(query, query_type="hybrid").rerank(reranker).to_arrow()

    References
    ----------
    Cormack et al. (2009). "Reciprocal rank fusion outperforms condorcet
    and individual rank learning methods." SIGIR '09.
    https://plg.uwaterloo.ca/~gvcormac/cormacksigir09-rrf.pdf
    """

    def __init__(self, K: int = 60, return_score="relevance"):
        if K <= 0:
            raise ValueError("K must be greater than 0")
        super().__init__(return_score)
        self.K = K

    def __str__(self):
        return f"RRFReranker(K={self.K})"

    def rerank_vector(self, query: str, vector_results: pa.Table) -> pa.Table:
        """
        Rerank vector search results using RRF based on result position.

        Parameters
        ----------
        query : str
            The query string (unused in RRF)
        vector_results : pa.Table
            Vector search results with _distance column

        Returns
        -------
        pa.Table
            Results with _relevance_score column, sorted descending
        """
        # Calculate RRF scores based on rank
        vector_ids = vector_results.column("_rowid").to_pylist()
        rrf_score_map = {}
        for i, result_id in enumerate(vector_ids):
            rrf_score_map[result_id] = 1.0 / (i + self.K)

        # Add relevance scores
        relevance_scores = [rrf_score_map[row_id] for row_id in vector_ids]
        vector_results = vector_results.append_column(
            "_relevance_score", pa.array(relevance_scores, type=pa.float32())
        )

        # Sort and handle score visibility
        vector_results = vector_results.sort_by([("_relevance_score", "descending")])

        if self.score == "relevance":
            vector_results = self._keep_relevance_score(vector_results)

        return vector_results

    def rerank_fts(self, query: str, fts_results: pa.Table) -> pa.Table:
        """
        Rerank FTS search results using RRF based on result position.

        Parameters
        ----------
        query : str
            The query string (unused in RRF)
        fts_results : pa.Table
            FTS search results with _score column

        Returns
        -------
        pa.Table
            Results with _relevance_score column, sorted descending
        """
        # Handle empty results
        fts_results = self._handle_empty_results(fts_results)

        # Calculate RRF scores based on rank
        fts_ids = fts_results.column("_rowid").to_pylist()
        rrf_score_map = {}
        for i, result_id in enumerate(fts_ids):
            rrf_score_map[result_id] = 1.0 / (i + self.K)

        # Add relevance scores
        relevance_scores = [rrf_score_map[row_id] for row_id in fts_ids]
        fts_results = fts_results.append_column(
            "_relevance_score", pa.array(relevance_scores, type=pa.float32())
        )

        # Sort and handle score visibility
        fts_results = fts_results.sort_by([("_relevance_score", "descending")])

        if self.score == "relevance":
            fts_results = self._keep_relevance_score(fts_results)

        return fts_results

    def rerank_hybrid(
        self,
        query: str,  # noqa: F821
        vector_results: pa.Table,
        fts_results: pa.Table,
    ):
        vector_ids = vector_results["_rowid"].to_pylist() if vector_results else []
        fts_ids = fts_results["_rowid"].to_pylist() if fts_results else []
        rrf_score_map = defaultdict(float)

        # Calculate RRF score of each result
        for ids in [vector_ids, fts_ids]:
            for i, result_id in enumerate(ids, 1):
                rrf_score_map[result_id] += 1 / (i + self.K)

        # Sort the results based on RRF score
        combined_results = self.merge_results(vector_results, fts_results)
        combined_row_ids = combined_results["_rowid"].to_pylist()
        relevance_scores = [rrf_score_map[row_id] for row_id in combined_row_ids]
        combined_results = combined_results.append_column(
            "_relevance_score", pa.array(relevance_scores, type=pa.float32())
        )
        combined_results = combined_results.sort_by(
            [("_relevance_score", "descending")]
        )

        if self.score == "relevance":
            combined_results = self._keep_relevance_score(combined_results)

        return combined_results

    def rerank_multivector(
        self,
        vector_results: Union[List[pa.Table], List["LanceVectorQueryBuilder"]],
        query: str = None,
        deduplicate: bool = True,  # noqa: F821 # TODO: automatically deduplicates
    ):
        """
        Overridden method to rerank the results from multiple vector searches.
        This leverages the RRF hybrid reranking algorithm to combine the
        results from multiple vector searches as it doesn't support reranking
        vector results individually.
        """
        # Make sure all elements are of the same type
        if not all(isinstance(v, type(vector_results[0])) for v in vector_results):
            raise ValueError(
                "All elements in vector_results should be of the same type"
            )

        # avoid circular import
        if type(vector_results[0]).__name__ == "LanceVectorQueryBuilder":
            vector_results = [result.to_arrow() for result in vector_results]
        elif not isinstance(vector_results[0], pa.Table):
            raise ValueError(
                "vector_results should be a list of pa.Table or LanceVectorQueryBuilder"
            )

        # _rowid is required for RRF reranking
        if not all("_rowid" in result.column_names for result in vector_results):
            raise ValueError(
                "'_rowid' is required for deduplication. \
                    add _rowid to search results like this: \
                    `search().with_row_id(True)`"
            )

        combined = pa.concat_tables(vector_results, **self._concat_tables_args)
        empty_table = pa.Table.from_arrays([], names=[])
        reranked = self.rerank_hybrid(query, combined, empty_table)

        return reranked
