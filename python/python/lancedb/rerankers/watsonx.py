# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors


import os
from functools import cached_property
from typing import Dict, Optional

import pyarrow as pa

from ..util import attempt_import_or_raise
from .base import Reranker

DEFAULT_WATSONX_URL = "https://us-south.ml.cloud.ibm.com"


class WatsonxReranker(Reranker):
    """
    Reranks the results using the IBM watsonx.ai Rerank API.

    Uses the ``ibm_watsonx_ai`` SDK (``Rerank.generate``) under the hood.

    API Docs:
        https://cloud.ibm.com/docs/apis/watsonx-ai#text-rerank

    Supported rerank models:
        https://dataplatform.cloud.ibm.com/docs/content/wsj/analyze-data/fm-models-embed.html?context=wx#rerank

    Parameters
    ----------
    model_name : str, default "cross-encoder/ms-marco-minilm-l-12-v2"
        The ID of the rerank model to use.
    column : str, default "text"
        The name of the column to use as input to the reranker.
    top_n : int, optional
        Return only the top-n results. If ``None``, all results are returned.
    return_score : str, default "relevance"
        Options are ``"relevance"`` or ``"all"``.
    api_key : str, optional
        IBM Cloud API key.  Falls back to the ``WATSONX_API_KEY`` environment
        variable when not provided.
    project_id : str, optional
        watsonx.ai project ID.  Explicit value takes precedence over the
        ``WATSONX_PROJECT_ID`` environment variable.  Mutually exclusive with
        ``space_id`` — exactly one must be supplied.
    space_id : str, optional
        watsonx.ai deployment space ID.  Explicit value takes precedence over
        the ``WATSONX_SPACE_ID`` environment variable.  Mutually exclusive with
        ``project_id`` — exactly one must be supplied.
    url : str, optional
        watsonx.ai service URL.  Defaults to
        ``"https://us-south.ml.cloud.ibm.com"``.
    truncate_input_tokens : int, optional
        Truncate each input to this many tokens before scoring.  Passed
        directly to the ``parameters`` dict of ``Rerank.generate``.
    """

    def __init__(
        self,
        model_name: str = "cross-encoder/ms-marco-minilm-l-12-v2",
        column: str = "text",
        top_n: Optional[int] = None,
        return_score: str = "relevance",
        api_key: Optional[str] = None,
        project_id: Optional[str] = None,
        space_id: Optional[str] = None,
        url: Optional[str] = None,
        truncate_input_tokens: Optional[int] = None,
    ):
        super().__init__(return_score)
        self.model_name = model_name
        self.column = column
        self.top_n = top_n
        self.api_key = api_key
        self.project_id = project_id
        self.space_id = space_id
        self.url = url
        self.truncate_input_tokens = truncate_input_tokens

    def __str__(self) -> str:
        return f"WatsonxReranker(model_name={self.model_name})"

    @cached_property
    def _client(self):
        ibm_watsonx_ai = attempt_import_or_raise("ibm_watsonx_ai")
        ibm_watsonx_ai_foundation_models = attempt_import_or_raise(
            "ibm_watsonx_ai.foundation_models"
        )

        # --- credentials ---
        api_key = self.api_key or os.environ.get("WATSONX_API_KEY")
        if not api_key:
            raise ValueError(
                "WATSONX_API_KEY not set. Either set it in your environment or "
                "pass it as `api_key` argument to WatsonxReranker."
            )
        credentials = ibm_watsonx_ai.Credentials(
            api_key=api_key,
            url=self.url or DEFAULT_WATSONX_URL,
        )

        # --- project_id / space_id (exactly one required) ---
        # Explicit field always wins; env vars are consulted only when neither
        # was passed explicitly, so a stray WATSONX_SPACE_ID never overrides an
        # explicit project_id and vice-versa.
        project_id = self.project_id if self.project_id is not None else None
        space_id = self.space_id if self.space_id is not None else None

        if project_id is None and space_id is None:
            # Neither was passed explicitly — fall back to env vars.
            project_id = os.environ.get("WATSONX_PROJECT_ID")
            space_id = os.environ.get("WATSONX_SPACE_ID")

        if project_id and space_id:
            raise ValueError("Provide either `project_id` or `space_id`, not both.")
        if not project_id and not space_id:
            raise ValueError(
                "Either WATSONX_PROJECT_ID or WATSONX_SPACE_ID must be set. "
                "Pass one as an argument to WatsonxReranker or set the corresponding "
                "environment variable."
            )

        kwargs: Dict = dict(model_id=self.model_name, credentials=credentials)
        if project_id:
            kwargs["project_id"] = project_id
        else:
            kwargs["space_id"] = space_id

        return ibm_watsonx_ai_foundation_models.Rerank(**kwargs)

    def _build_params(self) -> Dict:
        """Build the ``parameters`` dict forwarded to ``Rerank.generate``."""
        return_options: Dict = {"inputs": True}
        if self.top_n is not None:
            return_options["top_n"] = self.top_n
        params: Dict = {"return_options": return_options}
        if self.truncate_input_tokens is not None:
            params["truncate_input_tokens"] = self.truncate_input_tokens
        return params

    def _rerank(self, result_set: pa.Table, query: str) -> pa.Table:
        result_set = self._handle_empty_results(result_set)
        if len(result_set) == 0:
            return result_set

        docs = result_set[self.column].to_pylist()
        response = self._client.generate(
            query=query,
            inputs=docs,
            params=self._build_params(),
        )
        results = response["results"]

        indices, scores = zip(
            *[(result["index"], result["score"]) for result in results]
        )
        result_set = result_set.take(list(indices))
        result_set = result_set.append_column(
            "_relevance_score", pa.array(scores, type=pa.float32())
        )
        return result_set

    def rerank_hybrid(
        self,
        query: str,
        vector_results: pa.Table,
        fts_results: pa.Table,
    ) -> pa.Table:
        if self.score == "all":
            combined_results = self._merge_and_keep_scores(vector_results, fts_results)
        else:
            combined_results = self.merge_results(vector_results, fts_results)
        combined_results = self._rerank(combined_results, query)
        if self.score == "relevance":
            combined_results = self._keep_relevance_score(combined_results)
        return combined_results

    def rerank_vector(self, query: str, vector_results: pa.Table) -> pa.Table:
        vector_results = self._rerank(vector_results, query)
        if self.score == "relevance":
            vector_results = vector_results.drop_columns(["_distance"])
        return vector_results

    def rerank_fts(self, query: str, fts_results: pa.Table) -> pa.Table:
        fts_results = self._rerank(fts_results, query)
        if self.score == "relevance":
            fts_results = fts_results.drop_columns(["_score"])
        return fts_results
