# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors


import pyarrow as pa
from .base import Reranker
from ..util import attempt_import_or_raise


class AnswerdotaiRerankers(Reranker):
    """
    Reranks the results using the Answerdotai Rerank API.
    All supported reranker model types can be found here:
    - https://github.com/AnswerDotAI/rerankers


    Parameters
    ----------
    model_type : str, default "colbert"
        The type of the model to use.
    model_name : str, default "rerank-english-v2.0"
        The name of the model to use from the given model type.
    column : str, default "text"
        The name of the column to use as input to the cross encoder model.
    return_score : str, default "relevance"
        options are "relevance" or "all". Only "relevance" is supported for now.
    **kwargs
        Additional keyword arguments to pass to the model. For example, 'device'.
        See AnswerDotAI/rerankers for more information.
    """

    def __init__(
        self,
        model_type="colbert",
        model_name: str = "answerdotai/answerai-colbert-small-v1",
        column: str = "text",
        return_score="relevance",
        **kwargs,
    ):
        super().__init__(return_score)
        self.column = column
        rerankers = attempt_import_or_raise(
            "rerankers"
        )  # import here for faster ops later
        self.reranker = rerankers.Reranker(
            model_name=model_name, model_type=model_type, **kwargs
        )

    def _rerank(self, result_set: pa.Table, query: str):
        result_set = self._handle_empty_results(result_set)
        if len(result_set) == 0:
            return result_set
        docs = result_set[self.column].to_pylist()
        doc_ids = list(range(len(docs)))
        result = self.reranker.rank(query, docs, doc_ids=doc_ids)

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
            combined_results = self._merge_and_keep_scores(vector_results, fts_results)
        combined_results = combined_results.sort_by(
            [("_relevance_score", "descending")]
        )
        return combined_results

    def rerank_vector(self, query: str, vector_results: pa.Table):
        vector_results = self._rerank(vector_results, query)
        if self.score == "relevance":
            vector_results = vector_results.drop_columns(["_distance"])
        vector_results = vector_results.sort_by([("_relevance_score", "descending")])
        return vector_results

    def rerank_fts(self, query: str, fts_results: pa.Table):
        fts_results = self._rerank(fts_results, query)
        if self.score == "relevance":
            fts_results = fts_results.drop_columns(["_score"])
        fts_results = fts_results.sort_by([("_relevance_score", "descending")])
        return fts_results
