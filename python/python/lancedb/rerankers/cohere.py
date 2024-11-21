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

import os
from packaging.version import Version
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
        model_name: str = "rerank-english-v3.0",
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
        # ensure version is at least 0.5.0
        if hasattr(cohere, "__version__") and Version(cohere.__version__) < Version(
            "0.5.0"
        ):
            raise ValueError(
                f"cohere version must be at least 0.5.0, found {cohere.__version__}"
            )
        if os.environ.get("COHERE_API_KEY") is None and self.api_key is None:
            raise ValueError(
                "COHERE_API_KEY not set. Either set it in your environment or \
                pass it as `api_key` argument to the CohereReranker."
            )
        return cohere.Client(os.environ.get("COHERE_API_KEY") or self.api_key)

    def _rerank(self, result_set: pa.Table, query: str):
        docs = result_set[self.column].to_pylist()
        response = self._client.rerank(
            query=query,
            documents=docs,
            top_n=self.top_n,
            model=self.model_name,
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
                "return_score='all' not implemented for cohere reranker"
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
