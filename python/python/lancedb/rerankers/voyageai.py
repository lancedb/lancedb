# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors


import os
from functools import cached_property
from typing import List, Optional

import pyarrow as pa

from ..util import attempt_import_or_raise
from .base import Reranker, RerankableResult


class VoyageAIReranker(Reranker):
    """
    Reranks the results using the VoyageAI Rerank API.
    https://docs.voyageai.com/docs/reranker

    Parameters
    ----------
    model_name : str, default "rerank-english-v2.0"
        The name of the cross encoder model to use. Available voyageai models are:
        - rerank-2.5
        - rerank-2.5-lite
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

    def __str__(self):
        return f"VoyageAIReranker(model_name={self.model_name})"

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
        response = self._client.rerank(
            query=query,
            documents=docs,
            top_k=self.top_n,
            model=self.model_name,
            truncation=self.truncation,
        )
        # Build scores aligned to merged order (API may return fewer via top_n)
        merged_scores = [0.0] * len(docs)
        for r in response.results:
            merged_scores[r.index] = r.relevance_score

        text_to_score = {
            doc: score for doc, score in zip(docs, merged_scores)
        }

        score_arrays = []
        for result in results:
            texts = result.data[self.column].to_pylist()
            scores = [text_to_score.get(t, 0.0) for t in texts]
            score_arrays.append(pa.array(scores, type=pa.float32()))

        return score_arrays
