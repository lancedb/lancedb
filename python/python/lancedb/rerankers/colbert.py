import pyarrow as pa

from ..util import attempt_import_or_raise
from .base import Reranker


class ColbertReranker(Reranker):
    """
    Reranks the results using the ColBERT model.

    Parameters
    ----------
    model_name : str, default "colbert" (colbert-ir/colbert-v2.0)
        The name of the cross encoder model to use.
    column : str, default "text"
        The name of the column to use as input to the cross encoder model.
    return_score : str, default "relevance"
        options are "relevance" or "all". Only "relevance" is supported for now.
    """

    def __init__(
        self,
        model_name: str = "colbert",
        column: str = "text",
        return_score="relevance",
    ):
        super().__init__(return_score)
        self.model_name = model_name
        self.column = column
        rerankers = attempt_import_or_raise(
            "rerankers"
        )  # import here for faster ops later
        self.colbert = rerankers.Reranker(self.model_name, model_type="colbert")

    def _rerank(self, result_set: pa.Table, query: str):
        docs = result_set[self.column].to_pylist()
        doc_ids = list(range(len(docs)))
        result = self.colbert.rank(query, docs, doc_ids=doc_ids)

        # get the scores of each document in the same order as the input
        scores = [result.get_result_by_docid(i).score for i in doc_ids]

        # add the scores
        result_set = result_set.append_column(
            "_relevance_score", pa.array(scores, type=pa.float32())
        )

        return result_set

    def rerank_hybrid(
        self,
        query: str,
        vector_results: pa.Table,
        fts_results: pa.Table,
    ):
        combined_results = self.merge_results(vector_results, fts_results)
        combined_results = self._rerank(combined_results, query)
        if self.score == "relevance":
            combined_results = self._keep_relevance_score(combined_results)
        elif self.score == "all":
            raise NotImplementedError(
                "OpenAI Reranker does not support score='all' yet"
            )

        combined_results = combined_results.sort_by(
            [("_relevance_score", "descending")]
        )

        return combined_results

    def rerank_vector(
        self,
        query: str,
        vector_results: pa.Table,
    ):
        result_set = self._rerank(vector_results, query)
        if self.score == "relevance":
            result_set = result_set.drop_columns(["_distance"])

        result_set = result_set.sort_by([("_relevance_score", "descending")])

        return result_set

    def rerank_fts(
        self,
        query: str,
        fts_results: pa.Table,
    ):
        result_set = self._rerank(fts_results, query)
        if self.score == "relevance":
            result_set = result_set.drop_columns(["_score"])

        result_set = result_set.sort_by([("_relevance_score", "descending")])

        return result_set
