import os
from functools import cached_property
from typing import Union

import pyarrow as pa

from ..util import attempt_import_or_raise
from .base import Reranker


class CohereReranker(Reranker):
    """
    Reranks the results using the Cohere Rerank API.
    https://docs.cohere.com/docs/rerank-guide

    Parameters
    ----------
    model_name : str, default "rerank-english-v2.0"
        The name of the cross encoder model to use. Available cohere models are:
        - rerank-english-v2.0
        - rerank-multilingual-v2.0
    column : str, default "text"
        The name of the column to use as input to the cross encoder model.
    top_n : str, default None
        The number of results to return. If None, will return all results.
    """

    def __init__(
        self,
        model_name: str = "rerank-english-v2.0",
        column: str = "text",
        top_n: Union[int, None] = None,
        return_score="relevance",
        api_key: Union[str, None] = None,
    ):
        super().__init__(return_score)
        self.model_name = model_name
        self.column = column
        self.top_n = top_n
        self.api_key = api_key

    @cached_property
    def _client(self):
        cohere = attempt_import_or_raise("cohere")
        if os.environ.get("COHERE_API_KEY") is None and self.api_key is None:
            raise ValueError(
                "COHERE_API_KEY not set. Either set it in your environment or \
                pass it as `api_key` argument to the CohereReranker."
            )
        return cohere.Client(os.environ.get("COHERE_API_KEY") or self.api_key)

    def rerank_hybrid(
        self,
        query: str,
        vector_results: pa.Table,
        fts_results: pa.Table,
    ):
        combined_results = self.merge_results(vector_results, fts_results)
        docs = combined_results[self.column].to_pylist()
        results = self._client.rerank(
            query=query,
            documents=docs,
            top_n=self.top_n,
            model=self.model_name,
        )  # returns list (text, idx, relevance) attributes sorted descending by score
        indices, scores = list(
            zip(*[(result.index, result.relevance_score) for result in results])
        )  # tuples
        combined_results = combined_results.take(list(indices))
        # add the scores
        combined_results = combined_results.append_column(
            "_relevance_score", pa.array(scores, type=pa.float32())
        )

        if self.score == "relevance":
            combined_results = combined_results.drop_columns(["score", "_distance"])
        elif self.score == "all":
            raise NotImplementedError(
                "return_score='all' not implemented for cohere reranker"
            )
        return combined_results
