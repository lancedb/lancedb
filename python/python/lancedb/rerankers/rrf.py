import pyarrow as pa

from collections import defaultdict
from .base import Reranker


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
            combined_results = combined_results.drop_columns(["score", "_distance"])

        return combined_results
