# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors


from typing import List

import pyarrow as pa

from .base import Reranker, RerankableResult, VectorResult, FtsResult


class LinearCombinationReranker(Reranker):
    """
    Reranks the results using a linear combination of the scores from the
    vector and FTS search. For missing scores, fill with `fill` value.
    Parameters
    ----------
    weight : float, default 0.7
        The weight to give to the vector score. Must be between 0 and 1.
    fill : float, default 1.0
        The score to give to results that are only in one of the two result sets.
        This is treated as penalty, so a higher value means a lower score.
        TODO: We should just hardcode this--
        its pretty confusing as we invert scores to calculate final score
    return_score : str, default "relevance"
        opntions are "relevance" or "all"
        The type of score to return. If "relevance", will return only the relevance
        score. If "all", will return all scores from the vector and FTS search along
        with the relevance score.
    """

    def __init__(
        self, weight: float = 0.7, fill: float = 1.0, return_score="relevance"
    ):
        if weight < 0 or weight > 1:
            raise ValueError("weight must be between 0 and 1.")
        super().__init__(return_score)
        self.weight = weight
        self.fill = fill

    def __str__(self):
        return f"LinearCombinationReranker(weight={self.weight}, fill={self.fill})"

    def _combine_score(self, vector_score, fts_score):
        # these scores represent distance
        return 1 - (self.weight * vector_score + (1 - self.weight) * fts_score)

    def _invert_score(self, dist: float):
        # Invert the score between relevance and distance
        return 1 - dist

    def needs_columns(self):
        return ["_distance", "_score", "_rowid"]

    def compute_scores(
        self,
        query: str,
        results: List[RerankableResult],
    ) -> List[pa.Array]:
        # Collect per-rowid distance and score from typed result sets
        row_distance = {}  # rowid -> _distance
        row_fts_score = {}  # rowid -> _score

        for result in results:
            for row in result.data.to_pylist():
                rid = row.get("_rowid")
                if rid is None:
                    continue
                if isinstance(result, VectorResult):
                    if "_distance" in row and row["_distance"] is not None:
                        row_distance[rid] = row["_distance"]
                elif isinstance(result, FtsResult):
                    if "_score" in row and row["_score"] is not None:
                        row_fts_score[rid] = row["_score"]

        # Compute scores per result set, aligned to each table's rows
        score_arrays = []
        for result in results:
            scores = []
            for row in result.data.to_pylist():
                rid = row.get("_rowid")
                dist = row_distance.get(rid, self.fill)
                fts = row_fts_score.get(rid, self.fill)
                vector_score = self._invert_score(dist)
                scores.append(self._combine_score(vector_score, fts))
            score_arrays.append(pa.array(scores, type=pa.float32()))

        return score_arrays
