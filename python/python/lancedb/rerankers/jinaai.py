# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors


import os
from functools import cached_property
from typing import Union

import pyarrow as pa

from .base import Reranker

API_URL = "https://api.jina.ai/v1/rerank"


class JinaReranker(Reranker):
    """
    Reranks the results using the Jina Rerank API.
    https://jina.ai/rerank

    Parameters
    ----------
    model_name : str, default "jina-reranker-v2-base-multilingual"
        The name of the cross reanker model to use
    column : str, default "text"
        The name of the column to use as input to the cross encoder model.
    top_n : str, default None
        The number of results to return. If None, will return all results.
    api_key : str, default None
        The api key to access Jina API. If you pass None, you can set JINA_API_KEY
        environment variable
    """

    def __init__(
        self,
        model_name: str = "jina-reranker-v2-base-multilingual",
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
        import requests

        if os.environ.get("JINA_API_KEY") is None and self.api_key is None:
            raise ValueError(
                "JINA_API_KEY not set. Either set it in your environment or \
                pass it as `api_key` argument to the JinaReranker."
            )
        self.api_key = self.api_key or os.environ.get("JINA_API_KEY")
        self._session = requests.Session()
        self._session.headers.update(
            {"Authorization": f"Bearer {self.api_key}", "Accept-Encoding": "identity"}
        )
        return self._session

    def _rerank(self, result_set: pa.Table, query: str):
        result_set = self._handle_empty_results(result_set)
        if len(result_set) == 0:
            return result_set
        docs = result_set[self.column].to_pylist()
        response = self._client.post(  # type: ignore
            API_URL,
            json={
                "query": query,
                "documents": docs,
                "model": self.model_name,
                "top_n": self.top_n,
            },
        ).json()
        if "results" not in response:
            raise RuntimeError(response["detail"])

        results = response["results"]

        indices, scores = list(
            zip(*[(result["index"], result["relevance_score"]) for result in results])
        )  # tuples
        result_set = result_set.take(list(indices))
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
                "return_score='all' not implemented for JinaReranker"
            )
        return combined_results

    def rerank_vector(self, query: str, vector_results: pa.Table):
        vector_results = self._rerank(vector_results, query)
        if self.score == "relevance":
            vector_results = vector_results.drop_columns(["_distance"])
        return vector_results

    def rerank_fts(self, query: str, fts_results: pa.Table):
        fts_results = self._rerank(fts_results, query)
        if self.score == "relevance":
            fts_results = fts_results.drop_columns(["_score"])
        return fts_results
