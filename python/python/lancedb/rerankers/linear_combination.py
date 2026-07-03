# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors


from collections import defaultdict
from numpy import nan
import pyarrow as pa

from .base import Reranker


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
            # Convert vector distance to a relevance score in [0, 1] where
            # higher is better.  Missing vector entries are penalised with
            # `_invert_score(fill)` = 1 - fill (= 0.0 for the default fill=1).
            vector_score = self._invert_score(result.get("_distance", fill))
            # FTS scores (BM25) are already in a "higher = more relevant" space.
            # Missing FTS entries are penalised symmetrically: we use
            # `1 - fill` so that the same `fill` value drives both missing-vector
            # and missing-FTS penalties in the same direction.
            fts_score = result.get("_score", 1 - fill)
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
        # Both vector_score (inverted distance) and fts_score are in a
        # "higher = more relevant" space.  A straight weighted average gives
        # higher _relevance_score to better matches, as expected.
        # Previously this returned `1 - (...)` which inverted the final
        # ranking so that the *least* relevant document ranked first.
        return self.weight * vector_score + (1 - self.weight) * fts_score

    def _invert_score(self, dist: float):
        # Invert the score between relevance and distance
        return 1 - dist
