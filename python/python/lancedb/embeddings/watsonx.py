# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors


import os
from functools import cached_property
from typing import List, Optional, Dict, Union

from ..util import attempt_import_or_raise
from .base import TextEmbeddingFunction
from .registry import register

import numpy as np

DEFAULT_WATSONX_URL = "https://us-south.ml.cloud.ibm.com"

MODELS_DIMS = {
    # Current models
    "ibm/granite-embedding-278m-multilingual": 768,
    "ibm/slate-125m-english-rtrvr-v2": 768,
    "ibm/slate-30m-english-rtrvr-v2": 384,
    "intfloat/multilingual-e5-large": 1024,
    "sentence-transformers/all-minilm-l6-v2": 384,
    # Legacy model names — kept so that tables created with the old defaults
    # can still resolve their dimensions on load without errors.
    "ibm/slate-125m-english-rtrvr": 768,
    "ibm/slate-30m-english-rtrvr": 384,
    "sentence-transformers/all-minilm-l12-v2": 384,
}

# Models that are current (non-deprecated). Legacy entries above are intentionally
# excluded so that model_names() guides new users toward supported options.
_CURRENT_MODELS = [
    "ibm/granite-embedding-278m-multilingual",
    "ibm/slate-125m-english-rtrvr-v2",
    "ibm/slate-30m-english-rtrvr-v2",
    "intfloat/multilingual-e5-large",
    "sentence-transformers/all-minilm-l6-v2",
]


@register("watsonx")
class WatsonxEmbeddings(TextEmbeddingFunction):
    """
    An embedding function that uses the IBM watsonx.ai Embeddings API.

    API Docs:
        https://cloud.ibm.com/apidocs/watsonx-ai#text-embeddings

    Supported embedding models:
        https://dataplatform.cloud.ibm.com/docs/content/wsj/analyze-data/fm-models-embed.html?context=wx

    Parameters
    ----------
    name : str, default "ibm/granite-embedding-278m-multilingual"
        The ID of the embedding model to use.
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
    params : dict, optional
        Extra parameters forwarded verbatim to ``Embeddings`` (e.g.
        ``{"truncate_input_tokens": 512}``).
    """

    name: str = "ibm/granite-embedding-278m-multilingual"
    api_key: Optional[str] = None
    project_id: Optional[str] = None
    space_id: Optional[str] = None
    url: Optional[str] = None
    params: Optional[Dict] = None

    @staticmethod
    def sensitive_keys():
        return ["api_key"]

    @staticmethod
    def model_names():
        return list(_CURRENT_MODELS)

    def ndims(self):
        return self._ndims

    @cached_property
    def _ndims(self):
        if self.name not in MODELS_DIMS:
            raise ValueError(
                f"Unknown model '{self.name}'. Supported models: {_CURRENT_MODELS}"
            )
        return MODELS_DIMS[self.name]

    def generate_embeddings(
        self,
        texts: Union[List[str], np.ndarray],
        *args,
        **kwargs,
    ) -> List[List[float]]:
        return self._watsonx_client.embed_documents(
            texts=list(texts),
            *args,
            **kwargs,
        )

    @cached_property
    def _watsonx_client(self):
        ibm_watsonx_ai = attempt_import_or_raise("ibm_watsonx_ai")
        ibm_watsonx_ai_foundation_models = attempt_import_or_raise(
            "ibm_watsonx_ai.foundation_models"
        )

        # --- credentials ---
        # Explicit field takes priority; env var is the fallback.
        api_key = self.api_key or os.environ.get("WATSONX_API_KEY")
        if not api_key:
            raise ValueError(
                "WATSONX_API_KEY not set. Either set it in your environment or "
                "pass it as `api_key` argument to WatsonxEmbeddings."
            )
        credentials = ibm_watsonx_ai.Credentials(
            api_key=api_key,
            url=self.url or DEFAULT_WATSONX_URL,
        )

        # --- project_id / space_id (exactly one required) ---
        # Explicit field always wins; env var is consulted only when the
        # corresponding field was not set, so passing project_id= never
        # conflicts with a stray WATSONX_SPACE_ID env var and vice-versa.
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
                "Pass one as an argument to WatsonxEmbeddings or set the "
                "corresponding environment variable."
            )

        client_kwargs: Dict = dict(model_id=self.name, credentials=credentials)
        if self.params:
            client_kwargs["params"] = self.params
        if project_id:
            client_kwargs["project_id"] = project_id
        else:
            client_kwargs["space_id"] = space_id

        return ibm_watsonx_ai_foundation_models.Embeddings(**client_kwargs)
