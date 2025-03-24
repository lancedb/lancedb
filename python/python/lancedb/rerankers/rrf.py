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
    Reranks the results using Reciprocal Rank Fusion(RRF) algorithm based
    on the scores of vector and FTS search.
    Parameters
    ----------
    K : int, default 60
        A constant used in the RRF formula (default is 60). Experiments
        indicate that k = 60 was near-optimal, but that the choice is
        not critical. See paper:
        https://plg.uwaterloo.ca/~gvcormac/cormacksigir09-rrf.pdf
    return_score : str, default "relevance"
        opntions are "relevance" or "all"
        The type of score to return. If "relevance", will return only the relevance
        score. If "all", will return all scores from the vector and FTS search along
        with the relevance score.
    """

    def __init__(self, K: int = 60, return_score="relevance"):
        if K <= 0:
            raise ValueError("K must be greater than 0")
        super().__init__(return_score)
        self.K = K

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
