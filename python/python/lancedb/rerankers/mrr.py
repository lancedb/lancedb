# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors


from typing import Union, List, TYPE_CHECKING
import pyarrow as pa
import numpy as np

from collections import defaultdict
from .base import Reranker

if TYPE_CHECKING:
    from ..table import LanceVectorQueryBuilder


class MRRReranker(Reranker):
    """
    Reranks the results using Mean Reciprocal Rank (MRR) algorithm based
    on the scores of vector and FTS search.
    Algorithm reference - https://en.wikipedia.org/wiki/Mean_reciprocal_rank

    MRR calculates the average of reciprocal ranks across different search results.
    For each document, it computes the reciprocal of its rank in each system,
    then takes the mean of these reciprocal ranks as the final score.

    Parameters
    ----------
    weight_vector : float, default 0.5
        Weight for vector search results (0.0 to 1.0)
    weight_fts : float, default 0.5
        Weight for FTS search results (0.0 to 1.0)
        Note: weight_vector + weight_fts should equal 1.0
    return_score : str, default "relevance"
        Options are "relevance" or "all"
        The type of score to return. If "relevance", will return only the relevance
        score. If "all", will return all scores from the vector and FTS search along
        with the relevance score.
    """

    def __init__(
        self,
        weight_vector: float = 0.5,
        weight_fts: float = 0.5,
        return_score="relevance",
    ):
        if not (0.0 <= weight_vector <= 1.0):
            raise ValueError("weight_vector must be between 0.0 and 1.0")
        if not (0.0 <= weight_fts <= 1.0):
            raise ValueError("weight_fts must be between 0.0 and 1.0")
        if abs(weight_vector + weight_fts - 1.0) > 1e-6:
            raise ValueError("weight_vector + weight_fts must equal 1.0")

        super().__init__(return_score)
        self.weight_vector = weight_vector
        self.weight_fts = weight_fts

    def rerank_hybrid(
        self,
        query: str,  # noqa: F821
        vector_results: pa.Table,
        fts_results: pa.Table,
    ):
        vector_ids = vector_results["_rowid"].to_pylist() if vector_results else []
        fts_ids = fts_results["_rowid"].to_pylist() if fts_results else []

        # Maps result_id to list of (type, reciprocal_rank)
        mrr_score_map = defaultdict(list)

        if vector_ids:
            for rank, result_id in enumerate(vector_ids, 1):
                reciprocal_rank = 1.0 / rank
                mrr_score_map[result_id].append(("vector", reciprocal_rank))

        if fts_ids:
            for rank, result_id in enumerate(fts_ids, 1):
                reciprocal_rank = 1.0 / rank
                mrr_score_map[result_id].append(("fts", reciprocal_rank))

        final_mrr_scores = {}
        for result_id, scores in mrr_score_map.items():
            vector_rr = 0.0
            fts_rr = 0.0

            for score_type, reciprocal_rank in scores:
                if score_type == "vector":
                    vector_rr = reciprocal_rank
                elif score_type == "fts":
                    fts_rr = reciprocal_rank

            # If a document doesn't appear, its reciprocal rank is 0
            weighted_mrr = self.weight_vector * vector_rr + self.weight_fts * fts_rr
            final_mrr_scores[result_id] = weighted_mrr

        combined_results = self.merge_results(vector_results, fts_results)
        combined_row_ids = combined_results["_rowid"].to_pylist()
        relevance_scores = [final_mrr_scores[row_id] for row_id in combined_row_ids]
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
        deduplicate: bool = True,  # noqa: F821
    ):
        """
        Reranks the results from multiple vector searches using MRR algorithm.
        Each vector search result is treated as a separate ranking system,
        and MRR calculates the mean of reciprocal ranks across all systems.
        This cannot reuse rerank_hybrid because MRR semantics require treating
        each vector result as a separate ranking system.
        """
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

        if not all("_rowid" in result.column_names for result in vector_results):
            raise ValueError(
                "'_rowid' is required for deduplication. \
                    add _rowid to search results like this: \
                    `search().with_row_id(True)`"
            )

        mrr_score_map = defaultdict(list)

        for result_table in vector_results:
            result_ids = result_table["_rowid"].to_pylist()
            for rank, result_id in enumerate(result_ids, 1):
                reciprocal_rank = 1.0 / rank
                mrr_score_map[result_id].append(reciprocal_rank)

        final_mrr_scores = {}
        for result_id, reciprocal_ranks in mrr_score_map.items():
            mean_rr = np.mean(reciprocal_ranks)
            final_mrr_scores[result_id] = mean_rr

        combined = pa.concat_tables(vector_results, **self._concat_tables_args)
        combined = self._deduplicate(combined)

        combined_row_ids = combined["_rowid"].to_pylist()

        relevance_scores = [final_mrr_scores[row_id] for row_id in combined_row_ids]
        combined = combined.append_column(
            "_relevance_score", pa.array(relevance_scores, type=pa.float32())
        )
        combined = combined.sort_by([("_relevance_score", "descending")])

        if self.score == "relevance":
            combined = self._keep_relevance_score(combined)

        return combined
