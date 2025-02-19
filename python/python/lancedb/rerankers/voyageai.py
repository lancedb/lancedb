# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors


import os
from functools import cached_property
from typing import Optional

import pyarrow as pa

from ..util import attempt_import_or_raise
from .base import Reranker


class VoyageAIReranker(Reranker):
    """
    Reranks the results using the VoyageAI Rerank API.
    https://docs.voyageai.com/docs/reranker

    Parameters
    ----------
    model_name : str, default "rerank-english-v2.0"
        The name of the cross encoder model to use. Available voyageai models are:
        - rerank-2
        - rerank-2-lite
    column : str, default "text"
        The name of the column to use as input to the cross encoder model.
    top_n : int, default None
        The number of results to return. If None, will return all results.
    return_score : str, default "relevance"
        options are "relevance" or "all". Only "relevance" is supported for now.
    api_key : str, default None
        The API key to use. If None, will use the OPENAI_API_KEY environment variable.
    truncation : Optional[bool], default None
    """

    def __init__(
        self,
        model_name: str,
        column: str = "text",
        top_n: Optional[int] = None,
        return_score="relevance",
        api_key: Optional[str] = None,
        truncation: Optional[bool] = True,
    ):
        super().__init__(return_score)
        self.model_name = model_name
        self.column = column
        self.top_n = top_n
        self.api_key = api_key
        self.truncation = truncation

    @cached_property
    def _client(self):
        voyageai = attempt_import_or_raise("voyageai")
        if os.environ.get("VOYAGE_API_KEY") is None and self.api_key is None:
            raise ValueError(
                "VOYAGE_API_KEY not set. Either set it in your environment or \
                pass it as `api_key` argument to the VoyageAIReranker."
            )
        return voyageai.Client(
            api_key=os.environ.get("VOYAGE_API_KEY") or self.api_key,
        )

    def _rerank(self, result_set: pa.Table, query: str):
        docs = result_set[self.column].to_pylist()
        response = self._client.rerank(
            query=query,
            documents=docs,
            top_k=self.top_n,
            model=self.model_name,
            truncation=self.truncation,
        )
        results = (
            response.results
        )  # returns list (text, idx, relevance) attributes sorted descending by score
        indices, scores = list(
            zip(*[(result.index, result.relevance_score) for result in results])
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
                "return_score='all' not implemented for voyageai reranker"
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

        return result_set

    def rerank_fts(
        self,
        query: str,
        fts_results: pa.Table,
    ):
        result_set = self._rerank(fts_results, query)
        if self.score == "relevance":
            result_set = result_set.drop_columns(["_score"])

        return result_set
