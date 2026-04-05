# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors


from typing import List

import pyarrow as pa

from collections import defaultdict
from .base import Reranker, RerankableResult, VectorResult, FtsResult


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

    def __str__(self):
        return (
            f"MRRReranker(weight_vector={self.weight_vector}, "
            f"weight_fts={self.weight_fts})"
        )

    def needs_columns(self):
        return ["_rowid"]

    def compute_scores(
        self,
        query: str,
        results: List[RerankableResult],
    ) -> List[pa.Array]:
        # Determine weights per result set
        # For hybrid (1 vector + 1 fts): use configured weights
        # For multi-vector (all VectorResult): equal weights across all
        num_results = len(results)

        has_vector = any(isinstance(r, VectorResult) for r in results)
        has_fts = any(isinstance(r, FtsResult) for r in results)
        is_hybrid = has_vector and has_fts

        # Calculate weighted reciprocal rank per rowid
        mrr_score_map = defaultdict(float)

        if is_hybrid:
            for result in results:
                weight = (
                    self.weight_vector
                    if isinstance(result, VectorResult)
                    else self.weight_fts
                )
                row_ids = result.data["_rowid"].to_pylist()
                for rank, row_id in enumerate(row_ids, 1):
                    mrr_score_map[row_id] += weight * (1.0 / rank)
        else:
            # Multi-vector or single: equal weight per result set
            weight = 1.0 / num_results if num_results > 0 else 1.0
            for result in results:
                row_ids = result.data["_rowid"].to_pylist()
                for rank, row_id in enumerate(row_ids, 1):
                    mrr_score_map[row_id] += weight * (1.0 / rank)

        # Build score arrays aligned to each input result set
        score_arrays = []
        for result in results:
            row_ids = result.data["_rowid"].to_pylist()
            scores = [mrr_score_map[rid] for rid in row_ids]
            score_arrays.append(pa.array(scores, type=pa.float32()))

        return score_arrays
