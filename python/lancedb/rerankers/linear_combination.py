from .base import Reranker
import pyarrow as pa


class LinearCombinationReranker(Reranker):
    """
    Reranks the results using a linear combination of the scores from the vector and FTS search.
    For missing scores, fill with `fill` value.
    Parameters
    ----------
    weight : float, default 0.5
        The weight to give to the vector score. Must be between 0 and 1.
    fill : float, default 1.0
        The score to give to results that are only in one of the two result sets.
    """

    def __init__(self, weight: float = 0.5, fill: float = 1.0):
        if weight < 0 or weight > 1:
            raise ValueError("weight must be between 0 and 1.")
        self.weight = weight
        self.fill = fill

    def rerank_hybrid(
        self,
        query_builder: "lancedb.HybridQueryBuilder",  # noqa: F821
        vector_results: pa.Table,
        fts_results: pa.Table,
    ):
        combined_results = self.merge_results(vector_results, fts_results, self.fill)

        return combined_results

    def merge_results(
        self, vector_results: pa.Table, fts_results: pa.Table, fill: float
    ):
        # If both are empty then just return an empty table
        if len(vector_results) == 0 and len(fts_results) == 0:
            return vector_results
        # If one is empty then return the other
        if len(vector_results) == 0:
            return fts_results
        if len(fts_results) == 0:
            return vector_results

        # sort both input tables on _rowid
        combined_list = []
        vector_list = vector_results.sort_by("_rowid").to_pylist()
        fts_list = fts_results.sort_by("_rowid").to_pylist()
        i, j = 0, 0
        while i < len(vector_list):
            if j >= len(fts_list):
                for vi in vector_list[i:]:
                    vi["_score"] = self._combine_score(vi["_rowid"], fill)
                    combined_list.append(vi)
                break

            vi = vector_list[i]
            fj = fts_list[j]
            if vi["_rowid"] == fj["_rowid"]:
                vi["_score"] = self._combine_score(vi["_rowid"], fj["_rowid"])
                combined_list.append(vi)
                i += 1
                j += 1
            elif vector_list[i]["_rowid"] < fts_list[j]["_rowid"]:
                vi["_score"] = self._combine_score(vi["_rowid"], fill)
                combined_list.append(vi)
                i += 1
            else:
                fj["_score"] = self._combine_score(fj["_rowid"], fill)
                combined_list.append(fj)
                j += 1
        if j < len(fts_list) - 1:
            for fj in fts_list[j:]:
                fj["_score"] = self._combine_score(fj["_rowid"], fill)
                combined_list.append(fj)
        return pa.Table.from_pylist(combined_list, schema=vector_results.schema)

    def _combine_score(self, score1, score2):
        return self.weight * score1 + (1 - self.weight) * score2
