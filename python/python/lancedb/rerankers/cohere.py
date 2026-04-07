# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors


import os
from packaging.version import Version
from functools import cached_property
from typing import List, Union

import pyarrow as pa

from ..util import attempt_import_or_raise
from .base import Reranker, RerankableResult


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

    def __str__(self):
        return f"CohereReranker(model_name={self.model_name})"

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
            top_n=self.top_n,
            model=self.model_name,
        )
        # Build scores aligned to input order (API may return fewer via top_n)
        merged_scores = [0.0] * len(docs)
        for r in response.results:
            merged_scores[r.index] = r.relevance_score

        # Map back by text content
        text_to_score = {
            doc: score for doc, score in zip(docs, merged_scores)
        }

        score_arrays = []
        for result in results:
            texts = result.data[self.column].to_pylist()
            scores = [text_to_score.get(t, 0.0) for t in texts]
            score_arrays.append(pa.array(scores, type=pa.float32()))

        return score_arrays
