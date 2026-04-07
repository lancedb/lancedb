# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors


from typing import List

import pyarrow as pa

from collections import defaultdict
from .base import Reranker, RerankableResult


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

    def __str__(self):
        return f"RRFReranker(K={self.K})"

    def needs_columns(self):
        return ["_rowid"]

    def compute_scores(
        self,
        query: str,
        results: List[RerankableResult],
    ) -> List[pa.Array]:
        # Calculate RRF score per rowid across all result sets
        rrf_score_map = defaultdict(float)
        for result in results:
            row_ids = result.data["_rowid"].to_pylist()
            for rank, row_id in enumerate(row_ids, 1):
                rrf_score_map[row_id] += 1 / (rank + self.K)

        # Build score arrays aligned to each input result set
        score_arrays = []
        for result in results:
            row_ids = result.data["_rowid"].to_pylist()
            scores = [rrf_score_map[rid] for rid in row_ids]
            score_arrays.append(pa.array(scores, type=pa.float32()))

        return score_arrays
