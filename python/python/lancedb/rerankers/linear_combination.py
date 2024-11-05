#  Copyright (c) 2023. LanceDB Developers
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

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

        # sort both input tables on _rowid
        combined_list = []
        vector_list = vector_results.sort_by("_rowid").to_pylist()
        fts_list = fts_results.sort_by("_rowid").to_pylist()
        i, j = 0, 0
        while i < len(vector_list):
            if j >= len(fts_list):
                for vi in vector_list[i:]:
                    vi["_relevance_score"] = self._combine_score(vi["_distance"], fill)
                    combined_list.append(vi)
                break

            vi = vector_list[i]
            fj = fts_list[j]
            # invert the fts score from relevance to distance
            inverted_fts_score = self._invert_score(fj["_score"])
            if vi["_rowid"] == fj["_rowid"]:
                vi["_relevance_score"] = self._combine_score(
                    vi["_distance"], inverted_fts_score
                )
                vi["_score"] = fj["_score"]  # keep the original score
                combined_list.append(vi)
                i += 1
                j += 1
            elif vector_list[i]["_rowid"] < fts_list[j]["_rowid"]:
                vi["_relevance_score"] = self._combine_score(vi["_distance"], fill)
                combined_list.append(vi)
                i += 1
            else:
                fj["_relevance_score"] = self._combine_score(inverted_fts_score, fill)
                combined_list.append(fj)
                j += 1
        if j < len(fts_list) - 1:
            for fj in fts_list[j:]:
                fj["_relevance_score"] = self._combine_score(inverted_fts_score, fill)
                combined_list.append(fj)

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

    def _combine_score(self, score1, score2):
        # these scores represent distance
        return 1 - (self.weight * score1 + (1 - self.weight) * score2)

    def _invert_score(self, score: float):
        # Invert the score between relevance and distance
        return 1 - score
