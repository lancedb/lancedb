# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors


import json
import os
from functools import cached_property
from typing import List, Optional

import pyarrow as pa

from ..util import attempt_import_or_raise
from .base import Reranker, RerankableResult


class OpenaiReranker(Reranker):
    """
    Reranks the results using the OpenAI API.
    WARNING: This is a prompt based reranker that uses chat model that is
    not a dedicated reranker API. This should be treated as experimental.

    Parameters
    ----------
    model_name : str, default "gpt-4-turbo-preview"
        The name of the cross encoder model to use.
    column : str, default "text"
        The name of the column to use as input to the cross encoder model.
    return_score : str, default "relevance"
        options are "relevance" or "all". Only "relevance" is supported for now.
    api_key : str, default None
        The API key to use. If None, will use the OPENAI_API_KEY environment variable.
    """

    def __init__(
        self,
        model_name: str = "gpt-4-turbo-preview",
        column: str = "text",
        return_score="relevance",
        api_key: Optional[str] = None,
    ):
        super().__init__(return_score)
        self.model_name = model_name
        self.column = column
        self.api_key = api_key

    def __str__(self):
        return f"OpenaiReranker(model_name={self.model_name})"

    @cached_property
    def _client(self):
        openai = attempt_import_or_raise(
            "openai"
        )  # TODO: force version or handle versions < 1.0
        if os.environ.get("OPENAI_API_KEY") is None and self.api_key is None:
            raise ValueError(
                "OPENAI_API_KEY not set. Either set it in your environment or \
                pass it as `api_key` argument to the CohereReranker."
            )
        return openai.OpenAI(api_key=os.environ.get("OPENAI_API_KEY") or self.api_key)

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
        response = self._client.chat.completions.create(
            model=self.model_name,
            response_format={"type": "json_object"},
            temperature=0,
            messages=[
                {
                    "role": "system",
                    "content": "You are an expert relevance ranker. Given a list of\
                        documents and a query, your job is to determine the relevance\
                        each document is for answering the query. Your output is JSON,\
                        which is a list of documents. Each document has two fields,\
                        content and relevance_score.  relevance_score is from 0.0 to\
                        1.0 indicating the relevance of the text to the given query.\
                        Make sure to include all documents in the response.",
                },
                {"role": "user", "content": f"Query: {query} Docs: {docs}"},
            ],
        )
        api_results = json.loads(response.choices[0].message.content)["documents"]
        text_to_score = {
            r["content"]: float(r["relevance_score"]) for r in api_results
        }

        score_arrays = []
        for result in results:
            texts = result.data[self.column].to_pylist()
            scores = [text_to_score.get(t, 0.0) for t in texts]
            score_arrays.append(pa.array(scores, type=pa.float32()))

        return score_arrays
