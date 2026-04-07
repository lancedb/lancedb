# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors


import os
from functools import cached_property
from typing import List, Union

import pyarrow as pa

from .base import Reranker, RerankableResult

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

    def __str__(self):
        return f"JinaReranker(model_name={self.model_name})"

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

    def needs_columns(self):
        return [self.column]

    def compute_scores(
        self,
        query: str,
        results: List[RerankableResult],
    ) -> List[pa.Array]:
        tables = [r.data for r in results]
        merged = pa.concat_tables(tables, **self._concat_tables_args)
        if "_rowid" in merged.column_names:
            merged = self._deduplicate(merged)

        if len(merged) == 0:
            return [pa.array([], type=pa.float32()) for _ in results]

        docs = merged[self.column].to_pylist()
        response = self._client.post(
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

        # Build scores aligned to merged order (API may return fewer via top_n)
        merged_scores = [0.0] * len(docs)
        for r in response["results"]:
            merged_scores[r["index"]] = r["relevance_score"]

        text_to_score = {
            doc: score for doc, score in zip(docs, merged_scores)
        }

        score_arrays = []
        for result in results:
            texts = result.data[self.column].to_pylist()
            scores = [text_to_score.get(t, 0.0) for t in texts]
            score_arrays.append(pa.array(scores, type=pa.float32()))

        return score_arrays
