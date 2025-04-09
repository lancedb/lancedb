# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors


import json
import os
from functools import cached_property
from typing import Optional

import pyarrow as pa

from ..util import attempt_import_or_raise
from .base import Reranker


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

    def _rerank(self, result_set: pa.Table, query: str):
        result_set = self._handle_empty_results(result_set)
        if len(result_set) == 0:
            return result_set
        docs = result_set[self.column].to_pylist()
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
        results = json.loads(response.choices[0].message.content)["documents"]
        docs, scores = list(
            zip(*[(result["content"], result["relevance_score"]) for result in results])
        )  # tuples
        # replace the self.column column with the docs
        result_set = result_set.drop(self.column)
        result_set = result_set.append_column(
            self.column, pa.array(docs, type=pa.string())
        )
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
