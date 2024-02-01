from abc import ABC, abstractmethod

import numpy as np
import pyarrow as pa


class Reranker(ABC):
    def __init__(self, return_score: str = "relevance"):
        """
        Interface for a reranker. A reranker is used to rerank the results from a
        vector and FTS search. This is useful for combining the results from both
        search methods.

        Parameters
        ----------
        return_score : str, default "relevance"
            opntions are "relevance" or "all"
            The type of score to return. If "relevance", will return only the relevance
            score. If "all", will return all scores from the vector and FTS search along
            with the relevance score.

        """
        if return_score not in ["relevance", "all"]:
            raise ValueError("score must be either 'relevance' or 'all'")
        self.score = return_score

    @abstractmethod
    def rerank_hybrid(
        query: str,
        vector_results: pa.Table,
        fts_results: pa.Table,
    ):
        """
        Rerank function receives the individual results from the vector and FTS search
        results. You can choose to use any of the results to generate the final results,
        allowing maximum flexibility. This is mandatory to implement

        Parameters
        ----------
        query : str
            The input query
        vector_results : pa.Table
            The results from the vector search
        fts_results : pa.Table
            The results from the FTS search
        """
        pass

    def merge_results(self, vector_results: pa.Table, fts_results: pa.Table):
        """
        Merge the results from the vector and FTS search. This is a vanilla merging
        function that just concatenates the results and removes the duplicates.

        NOTE: This doesn't take score into account. It'll keep the instance that was
        encountered first. This is designed for rerankers that don't use the score.
        In case you want to use the score, or support `return_scores="all"` you'll
        have to implement your own merging function.

        Parameters
        ----------
        vector_results : pa.Table
            The results from the vector search
        fts_results : pa.Table
            The results from the FTS search
        """
        combined = pa.concat_tables([vector_results, fts_results], promote=True)
        row_id = combined.column("_rowid")

        # deduplicate
        mask = np.full((combined.shape[0]), False)
        _, mask_indices = np.unique(np.array(row_id), return_index=True)
        mask[mask_indices] = True
        combined = combined.filter(mask=mask)

        return combined
