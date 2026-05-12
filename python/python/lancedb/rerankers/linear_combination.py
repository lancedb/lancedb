# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors


from collections import defaultdict
from numpy import nan
import numpy as np
import pyarrow as pa

from .base import Reranker


class LinearCombinationReranker(Reranker):
    """
    Reranks the results using a linear combination of the scores from the
    vector and FTS search.

    This reranker can be used for:
    - Vector search reranking
    - Full-text search reranking
    - Hybrid search (vector + FTS) reranking

    For hybrid search, missing scores are filled with the `fill` value.

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
        options are "relevance" or "all"
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

    def rerank_vector(self, query: str, vector_results: pa.Table) -> pa.Table:
        """
        Rerank vector results using linear combination.
        Since there's no FTS score, use weight=1.0 for vector score.

        Parameters
        ----------
        query : str
            The query string (unused)
        vector_results : pa.Table
            Vector search results with _distance column

        Returns
        -------
        pa.Table
            Results with _relevance_score column, sorted descending
        """
        # Normalize distance to similarity score
        distances = vector_results.column("_distance").to_numpy()
        max_dist = distances.max() if len(distances) > 0 else 1.0
        min_dist = distances.min() if len(distances) > 0 else 0.0
        rng = max_dist - min_dist if max_dist - min_dist > 1e-5 else 1.0

        # Normalize and invert (distance -> similarity)
        if rng != 0.0:
            normalized = (distances - min_dist) / rng
            relevance_scores = 1.0 - normalized  # Invert
        else:
            relevance_scores = np.ones_like(distances)

        # Add relevance score column
        vector_results = vector_results.append_column(
            "_relevance_score", pa.array(relevance_scores, type=pa.float32())
        )

        # Sort descending
        vector_results = vector_results.sort_by([("_relevance_score", "descending")])

        if self.score == "relevance":
            vector_results = self._keep_relevance_score(vector_results)

        return vector_results

    def rerank_fts(self, query: str, fts_results: pa.Table) -> pa.Table:
        """
        Rerank FTS results using linear combination.
        Since there's no vector score, directly use FTS scores.

        Parameters
        ----------
        query : str
            The query string (unused)
        fts_results : pa.Table
            FTS search results with _score column

        Returns
        -------
        pa.Table
            Results with _relevance_score column, sorted descending
        """
        # FTS scores are already normalized (BM25 scores)
        # Just use them as relevance scores
        scores = fts_results.column("_score").to_numpy()

        # Normalize scores to [0, 1]
        max_score = scores.max() if len(scores) > 0 else 1.0
        min_score = scores.min() if len(scores) > 0 else 0.0
        rng = max_score - min_score if max_score - min_score > 1e-5 else 1.0

        if rng != 0.0:
            relevance_scores = (scores - min_score) / rng
        else:
            relevance_scores = np.ones_like(scores)

        # Add relevance score column
        fts_results = fts_results.append_column(
            "_relevance_score", pa.array(relevance_scores, type=pa.float32())
        )

        # Sort descending
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
        combined_results = self.merge_results(vector_results, fts_results, self.fill)

        return combined_results

    def merge_results(
        self, vector_results: pa.Table, fts_results: pa.Table, fill: float
    ):
        # If one is empty then return the other and add _relevance_score
        # column equal the existing vector or fts score
        if len(vector_results) == 0:
            results = fts_results.append_column(
                "_relevance_score",
                pa.array(fts_results["_score"], type=pa.float32()),
            )
            if self.score == "relevance":
                results = self._keep_relevance_score(results)
            elif self.score == "all":
                results = results.append_column(
                    "_distance",
                    pa.array([nan] * len(fts_results), type=pa.float32()),
                )
            return results

        if len(fts_results) == 0:
            # invert the distance to relevance score
            results = vector_results.append_column(
                "_relevance_score",
                pa.array(
                    [
                        self._invert_score(distance)
                        for distance in vector_results["_distance"].to_pylist()
                    ],
                    type=pa.float32(),
                ),
            )
            if self.score == "relevance":
                results = self._keep_relevance_score(results)
            elif self.score == "all":
                results = results.append_column(
                    "_score",
                    pa.array([nan] * len(vector_results), type=pa.float32()),
                )
            return results
        results = defaultdict()
        for vector_result in vector_results.to_pylist():
            results[vector_result["_rowid"]] = vector_result
        for fts_result in fts_results.to_pylist():
            row_id = fts_result["_rowid"]
            if row_id in results:
                results[row_id]["_score"] = fts_result["_score"]
            else:
                results[row_id] = fts_result

        combined_list = []
        for row_id, result in results.items():
            vector_score = self._invert_score(result.get("_distance", fill))
            fts_score = result.get("_score", fill)
            result["_relevance_score"] = self._combine_score(vector_score, fts_score)
            combined_list.append(result)

        relevance_score_schema = pa.schema(
            [
                pa.field("_relevance_score", pa.float32()),
            ]
        )
        combined_schema = pa.unify_schemas(
            [vector_results.schema, fts_results.schema, relevance_score_schema]
        )
        tbl = pa.Table.from_pylist(combined_list, schema=combined_schema).sort_by(
            [("_relevance_score", "descending")]
        )
        if self.score == "relevance":
            tbl = self._keep_relevance_score(tbl)
        return tbl

    def _combine_score(self, vector_score, fts_score):
        # these scores represent distance
        return 1 - (self.weight * vector_score + (1 - self.weight) * fts_score)

    def _invert_score(self, dist: float):
        # Invert the score between relevance and distance
        return 1 - dist
